"""Platform-agnostic migration export / import.

This module is intentionally self-contained: it owns its own state, helpers,
and business logic so that the backup application (``app.py``) only needs to
wire up the HTTP routes and pass in the shared ``OperationLock``.

**Export** creates a portable bundle (manifest.json + app data + checksums)
on any rclone remote.  **Import** restores from such a bundle, either
directly to the local filesystem or via a remote router API.  The bundle
format is platform-agnostic — it records source metadata but does not
require the target to be the same platform.

The remote-instance import path talks to the OpenHost router API.  If you
need to support a different platform, the ``_import_to_remote_instance``
function is the only thing that needs an adapter.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
import re
import shutil
import sqlite3
import tarfile
import tempfile
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import httpx
import os
import subprocess

from operations import OpKind, OperationLock

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MIGRATION_BUNDLE_VERSION = 3
MIGRATION_DIR_PREFIX = "migration-"
# Allow alphanumerics, hyphens, dots, underscores, and colons (for timestamps)
MIGRATION_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._:T-]*$")


# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
# Progress / log state lives here rather than in app.py.  The route handlers
# expose these via the ``/api/migration/status`` endpoint.

status: dict | None = None  # {"phase": ..., "progress": ..., ...}
log: list[str] = []


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def validate_name(name: str) -> bool:
    """Validate a migration bundle name or label to prevent path traversal."""
    if not name or len(name) > 200:
        return False
    if ".." in name or "/" in name or "\\" in name:
        return False
    return bool(MIGRATION_NAME_RE.match(name))


def _strip_url_credentials(url: str | None) -> str | None:
    """Remove embedded credentials (e.g. x-access-token) from a URL."""
    if not url or not url.startswith("http"):
        return url
    try:
        parsed = urllib.parse.urlparse(url)
        if parsed.username or parsed.password:
            netloc = parsed.hostname or ""
            if parsed.port:
                netloc += f":{parsed.port}"
            return urllib.parse.urlunparse(
                (
                    parsed.scheme,
                    netloc,
                    parsed.path,
                    parsed.params,
                    parsed.query,
                    parsed.fragment,
                )
            )
    except Exception:
        logger.warning("Failed to parse URL for credential stripping")
    return url


def _is_local_url(url: str) -> bool:
    """Check if a URL points to a local / internal address."""
    try:
        parsed = urllib.parse.urlparse(url)
        host = (parsed.hostname or "").lower()
        return host in (
            "localhost",
            "127.0.0.1",
            "::1",
            "host.docker.internal",
        ) or host.endswith(".local")
    except Exception:
        logger.warning("Failed to parse URL in _is_local_url: %s", url)
        return False


# ---------------------------------------------------------------------------
# Logging helper
# ---------------------------------------------------------------------------


def _log(msg: str) -> None:
    """Append a timestamped message to the in-memory migration log."""
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    log.append(f"[{ts}] {msg}")
    logger.info("migration: %s", msg)


# ---------------------------------------------------------------------------
# Router HTTP helpers  (OpenHost-specific)
# ---------------------------------------------------------------------------


async def _router_get(path: str, token: str | None = None, base_url: str = "") -> dict:
    url = base_url.rstrip("/") + path
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    skip_verify = _is_local_url(base_url)
    async with httpx.AsyncClient(verify=not skip_verify, timeout=60) as client:
        r = await client.get(url, headers=headers)
        r.raise_for_status()
        ct = r.headers.get("content-type", "")
        if "json" in ct:
            return r.json()
        return {"ok": True, "text": r.text}


async def _router_post(
    path: str,
    data: dict | None = None,
    token: str | None = None,
    base_url: str = "",
) -> dict:
    url = base_url.rstrip("/") + path
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    skip_verify = _is_local_url(base_url)
    async with httpx.AsyncClient(verify=not skip_verify, timeout=120) as client:
        r = await client.post(url, data=data or {}, headers=headers)
        r.raise_for_status()
        ct = r.headers.get("content-type", "")
        if "json" in ct:
            return r.json()
        return {"ok": True, "text": r.text}


# ---------------------------------------------------------------------------
# App-metadata discovery
# ---------------------------------------------------------------------------


def _parse_git_remote_url(git_config_path: Path) -> str | None:
    """Extract the origin remote URL from a .git/config file."""
    try:
        text = git_config_path.read_text()
    except OSError:
        return None
    in_origin = False
    for line in text.splitlines():
        stripped = line.strip()
        if stripped == '[remote "origin"]':
            in_origin = True
        elif stripped.startswith("["):
            in_origin = False
        elif in_origin and stripped.startswith("url ="):
            return stripped.split("=", 1)[1].strip()
    return None


async def get_apps_metadata(
    vm_data_dir: Path,
    router_url: str,
    token: str | None = None,
    base_url: str | None = None,
) -> list[dict]:
    """Return app metadata from the local router DB or the router API.

    Tries the local router.db first (for richer metadata), falling back
    to the router HTTP API if the DB is missing, empty, or invalid.
    """
    apps: list[dict] = []
    router_db = vm_data_dir / "router.db"

    # Try local DB if it exists and is non-empty
    if router_db.exists() and router_db.stat().st_size > 0:

        def _read():
            conn = sqlite3.connect(str(router_db))
            conn.row_factory = sqlite3.Row
            try:
                # Check that the apps table exists before querying
                tables = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='apps'"
                ).fetchone()
                if not tables:
                    return None  # Signal to fall back to API
                rows = conn.execute(
                    "SELECT name, manifest_name, version, description, repo_url, "
                    "health_check, local_port, container_port, status, memory_mb, "
                    "cpu_millicores, gpu, public_paths, manifest_raw, runtime_type "
                    "FROM apps ORDER BY name"
                ).fetchall()
                return [dict(row) for row in rows]
            finally:
                conn.close()

        result = await asyncio.to_thread(_read)
        if result is not None:
            apps = result
            return apps

    # Fall back to the router HTTP API
    data = await _router_get("/api/apps", token=token, base_url=base_url or router_url)
    if isinstance(data, dict):
        for name, info in data.items():
            apps.append(
                {
                    "name": name,
                    "status": info.get("status"),
                    "repo_url": None,
                    "manifest_raw": None,
                }
            )

    # Enrich apps with repo_url from git repos in temp data if available.
    # The router API doesn't expose repo_url, but each app's cloned repo
    # is at /data/app_temp_data/{name}/repo/.git/config.
    if apps:
        app_temp_base = Path("/data/app_temp_data")
        for app_info in apps:
            if app_info.get("repo_url"):
                continue
            name = app_info["name"]
            git_config = app_temp_base / name / "repo" / ".git" / "config"
            if git_config.exists():
                try:
                    repo_url = await asyncio.to_thread(
                        _parse_git_remote_url, git_config
                    )
                    if repo_url:
                        app_info["repo_url"] = repo_url
                except Exception:
                    pass

    return apps


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------


def _safe_copy_sqlite(src: Path, dst: Path) -> None:
    src_conn = sqlite3.connect(str(src))
    try:
        dst_conn = sqlite3.connect(str(dst))
        try:
            src_conn.backup(dst_conn)
        finally:
            dst_conn.close()
    finally:
        src_conn.close()


def _compute_file_checksums_sync(directory: Path) -> dict[str, str]:
    checksums: dict[str, str] = {}
    if not directory.exists():
        return checksums
    for fpath in sorted(directory.rglob("*")):
        if fpath.is_file():
            try:
                h = hashlib.sha256()
                with open(fpath, "rb") as f:
                    for chunk in iter(lambda: f.read(8192), b""):
                        h.update(chunk)
                checksums[fpath.relative_to(directory).as_posix()] = h.hexdigest()
            except OSError as e:
                rel = fpath.relative_to(directory).as_posix()
                logger.warning("Could not checksum %s: %s", rel, e)
    return checksums


async def _compute_file_checksums(directory: Path) -> dict[str, str]:
    return await asyncio.to_thread(_compute_file_checksums_sync, directory)


# ---------------------------------------------------------------------------
# Permission-fixing helpers
# ---------------------------------------------------------------------------
# After restoring data via rclone or tar extraction, files may be owned by
# root or a different uid than the host user that runs the OpenHost router.
# The router's _fix_ownership() uses Docker to chown, but only during
# provision_data().  We replicate that pattern here so restored data has
# correct ownership and mode for the router to manage.


def _fix_dir_permissions_sync(directory: Path) -> None:
    """Fix ownership and mode of *directory* so the host user can manage it.

    1. chmod 0o777 on the directory itself (matching OpenHost's _ensure_dir).
    2. Use Docker alpine to chown -R to the current uid:gid, matching how
       the OpenHost router fixes ownership in core/data.py.
    """
    if not directory.exists():
        return

    uid, gid = os.getuid(), os.getgid()

    # Try simple chmod first — works if we already own the dir
    try:
        os.chmod(directory, 0o777)
    except PermissionError:
        pass

    # Use Docker to chown recursively (same pattern as OpenHost router)
    try:
        result = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "-v",
                f"{directory}:/d",
                "alpine",
                "chown",
                "-R",
                f"{uid}:{gid}",
                "/d",
            ],
            capture_output=True,
            timeout=120,
        )
        if result.returncode != 0:
            logger.warning(
                "Docker chown failed for %s (exit %d): %s",
                directory,
                result.returncode,
                result.stderr.decode().strip()[:200],
            )
    except FileNotFoundError:
        # Docker not available (shouldn't happen in an OpenHost container)
        logger.warning("Docker not available for chown; trying chmod fallback")
        try:
            subprocess.run(
                ["chmod", "-R", "a+rwX", str(directory)],
                capture_output=True,
                timeout=120,
            )
        except Exception as e:
            logger.warning("chmod fallback also failed: %s", e)
    except subprocess.TimeoutExpired:
        logger.warning("Docker chown timed out for %s", directory)

    # Ensure the top-level dir has 0o777
    try:
        os.chmod(directory, 0o777)
    except PermissionError:
        pass


async def _fix_app_data_permissions(
    app_names: list[str],
    all_app_data: Path,
) -> None:
    """Fix permissions on all restored app data directories."""
    for app_name in app_names:
        app_dir = all_app_data / app_name
        if app_dir.exists():
            _log(f"Fixing permissions for: {app_name}")
            await asyncio.to_thread(_fix_dir_permissions_sync, app_dir)
    _log("Permissions fixed for restored app data")


def _build_manifest(
    apps: list[dict],
    zone_domain: str,
    checksums: dict[str, str] | None = None,
) -> dict:
    return {
        "version": MIGRATION_BUNDLE_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_instance": zone_domain or "unknown",
        "source_platform": "openhost",
        "apps": [
            {
                "name": a["name"],
                "repo_url": _strip_url_credentials(a.get("repo_url")),
                "version": a.get("version"),
                "description": a.get("description"),
                "manifest_raw": a.get("manifest_raw"),
                "memory_mb": a.get("memory_mb"),
                "cpu_millicores": a.get("cpu_millicores"),
                "runtime_type": a.get("runtime_type"),
                "status": a.get("status"),
            }
            for a in apps
        ],
        "checksums": checksums or {},
    }


# ---------------------------------------------------------------------------
# Shared rclone data-restore helper
# ---------------------------------------------------------------------------


async def _restore_app_data(
    bundle_base: str,
    app_names: list[str],
    all_app_data: Path,
    rclone_conf: Path,
    progress_base: int,
    progress_span: int,
) -> None:
    global status

    for i, app_name in enumerate(app_names):
        src = f"{bundle_base}/app_data/{app_name}"
        dst = str(all_app_data / app_name)
        _log(f"Restoring data for: {app_name}")

        proc = await asyncio.create_subprocess_exec(
            "rclone",
            "copy",
            src,
            dst,
            "--config",
            str(rclone_conf),
            "-v",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        stdout, _ = await proc.communicate()
        if proc.returncode != 0:
            detail = stdout.decode().strip()[-200:] if stdout else ""
            _log(
                f"Warning: data restore may have partial errors for {app_name}"
                + (f": {detail}" if detail else "")
            )
        else:
            _log(f"Data restored for {app_name}")

        pct = progress_base + int(progress_span * (i + 1) / len(app_names))
        status = {"phase": "restoring_data", "progress": pct, "current_app": app_name}

    # Fix permissions so the host router can manage restored data
    await _fix_app_data_permissions(app_names, all_app_data)


async def _verify_checksums(
    bundle_checksums: dict[str, str],
    app_names: list[str],
    all_app_data: Path,
) -> int:
    """Verify file integrity.  Returns mismatch count."""
    global status

    if not bundle_checksums:
        _log("No checksums in manifest, skipping integrity check")
        return 0

    _log("Verifying file integrity via checksums...")
    status = {"phase": "verifying_checksums", "progress": 85}

    local_checksums: dict[str, str] = {}
    for app_name in app_names:
        app_dir = all_app_data / app_name
        if app_dir.exists():
            sub = await _compute_file_checksums(app_dir)
            for rel, digest in sub.items():
                local_checksums[f"{app_name}/{rel}"] = digest

    app_prefixes = tuple(f"{name}/" for name in app_names)
    mismatches = 0
    total_checked = 0
    for rel_path, expected in bundle_checksums.items():
        if not rel_path.startswith(app_prefixes):
            continue
        total_checked += 1
        if not (all_app_data / rel_path).exists():
            _log(f"  missing: {rel_path}")
            mismatches += 1
            continue
        if local_checksums.get(rel_path, "") != expected:
            _log(f"  mismatch: {rel_path}")
            mismatches += 1

    if mismatches:
        _log(
            f"Checksum verification: {mismatches} issue(s) out of {total_checked} files checked"
        )
    else:
        _log(f"Checksum verification passed ({total_checked} files OK)")
    return mismatches


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------


async def run_export(
    *,
    app_filter: str | None,
    name: str | None,
    lock: OperationLock,
    rclone_conf: Path,
    all_app_data: Path,
    vm_data_dir: Path,
    router_url: str,
    zone_domain: str,
    load_config: callable,
    router_token: str | None = None,
) -> bool:
    """Export a portable migration bundle to the configured rclone remote."""
    global status
    lock.try_acquire(OpKind.MIGRATION)  # already acquired by route handler
    log.clear()
    status = {"phase": "starting", "progress": 0}

    conf = load_config()
    remote_name = conf["remote_name"]
    remote_path = conf["remote_path"]

    try:
        if not rclone_conf.exists():
            raise RuntimeError("No rclone.conf configured")

        # 1. Gather app metadata
        _log("Gathering app metadata...")
        status = {"phase": "gathering_metadata", "progress": 5}
        apps = await get_apps_metadata(vm_data_dir, router_url, token=router_token)

        if app_filter:
            apps = [a for a in apps if a["name"] == app_filter]
            if not apps:
                raise RuntimeError(f"App '{app_filter}' not found")
        else:
            apps = [a for a in apps if a["name"] != "backup"]

        if not apps:
            raise RuntimeError("No apps to export")

        manifest = _build_manifest(apps, zone_domain)
        app_names = [a["name"] for a in manifest["apps"]]
        _log(f"Found {len(app_names)} apps: {', '.join(app_names)}")

        # 2. Staging directory
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        if app_filter and name:
            bundle_name = f"{MIGRATION_DIR_PREFIX}app-{app_filter}-{name}-{timestamp}"
        elif app_filter:
            bundle_name = f"{MIGRATION_DIR_PREFIX}app-{app_filter}-{timestamp}"
        elif name:
            bundle_name = f"{MIGRATION_DIR_PREFIX}{name}-{timestamp}"
        else:
            bundle_name = f"{MIGRATION_DIR_PREFIX}{timestamp}"

        with tempfile.TemporaryDirectory() as tmpdir:
            staging = Path(tmpdir) / bundle_name
            staging.mkdir()

            status = {"phase": "copying_metadata", "progress": 10}
            _log("Preparing manifest (checksums pending)")

            # 3. Platform metadata (full-instance export only)
            if not app_filter:
                router_db = vm_data_dir / "router.db"
                if router_db.exists():
                    vm_staging = staging / "vm_data"
                    vm_staging.mkdir()
                    await asyncio.to_thread(
                        _safe_copy_sqlite, router_db, vm_staging / "router.db"
                    )
                    _log("Copied router.db (safe backup)")

                    keys_dir = vm_data_dir / "identity_keys"
                    if keys_dir.exists():
                        keys_staging = vm_staging / "identity_keys"
                        keys_staging.mkdir()

                        def _copy_keys():
                            for kf in keys_dir.iterdir():
                                if kf.is_file():
                                    shutil.copy2(kf, keys_staging / kf.name)

                        await asyncio.to_thread(_copy_keys)
                        _log("Copied identity keys")

            status = {"phase": "copying_app_data", "progress": 20}

            # 4. Copy app data
            app_data_staging = staging / "app_data"
            app_data_staging.mkdir()

            for i, app_name in enumerate(app_names):
                src = all_app_data / app_name
                if src.exists():
                    _log(f"Copying data for app: {app_name}")
                    dst = app_data_staging / app_name
                    proc = await asyncio.create_subprocess_exec(
                        "rclone",
                        "copy",
                        str(src),
                        str(dst),
                        "--config",
                        str(rclone_conf),
                        "-v",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.STDOUT,
                    )
                    stdout, _ = await proc.communicate()
                    if proc.returncode != 0:
                        detail = stdout.decode().strip()[-200:] if stdout else ""
                        _log(
                            f"Warning: failed to copy data for {app_name}"
                            + (f": {detail}" if detail else "")
                        )
                else:
                    _log(f"No data directory for app: {app_name}")

                pct = 20 + int(50 * (i + 1) / len(app_names))
                status = {
                    "phase": "copying_app_data",
                    "progress": pct,
                    "current_app": app_name,
                }

            # 5. Checksums
            _log("Computing file checksums for integrity verification...")
            status = {"phase": "computing_checksums", "progress": 72}
            checksums = await _compute_file_checksums(app_data_staging)
            manifest["checksums"] = checksums
            _log(f"Computed checksums for {len(checksums)} files")

            # 6. Write manifest
            (staging / "manifest.json").write_text(json.dumps(manifest, indent=2))
            _log("Wrote final manifest.json with checksums")

            # 7. Upload
            _log("Uploading migration bundle to remote storage...")
            status = {"phase": "uploading", "progress": 80}

            dest = f"{remote_name}:{remote_path}/migrations/{bundle_name}"
            proc = await asyncio.create_subprocess_exec(
                "rclone",
                "copy",
                str(staging),
                dest,
                "--config",
                str(rclone_conf),
                "-v",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            stdout, _ = await proc.communicate()
            if stdout:
                for line in stdout.decode().splitlines():
                    logger.info("rclone: %s", line)
            if proc.returncode != 0:
                raise RuntimeError(
                    f"rclone upload failed with exit code {proc.returncode}"
                )

            _log(f"Migration bundle uploaded: {bundle_name}")
            status = {"phase": "done", "progress": 100, "bundle": bundle_name}
            return True

    except Exception as e:
        _log(f"Export failed: {e}")
        status = {"phase": "error", "progress": 0, "error": str(e)}
        return False
    finally:
        lock.release(OpKind.MIGRATION)


# ---------------------------------------------------------------------------
# Import
# ---------------------------------------------------------------------------


async def run_import(
    *,
    bundle_name: str,
    target_url: str | None,
    target_token: str | None,
    selected_apps: list[str] | None,
    lock: OperationLock,
    rclone_conf: Path,
    all_app_data: Path,
    config_dir: Path,
    load_config: callable,
) -> bool:
    """Import a migration bundle (local or remote target)."""
    global status
    lock.try_acquire(OpKind.MIGRATION)  # already acquired by route handler
    log.clear()
    status = {"phase": "starting", "progress": 0}

    conf = load_config()
    remote_name = conf["remote_name"]
    remote_path = conf["remote_path"]
    is_remote = bool(target_url)

    try:
        _log(f"Downloading manifest from bundle: {bundle_name}")
        status = {"phase": "downloading_manifest", "progress": 5}

        manifest = await get_manifest(bundle_name, rclone_conf, load_config)
        if not manifest:
            raise RuntimeError("Could not download or parse manifest.json")

        apps_in_bundle = manifest.get("apps", [])
        if selected_apps:
            apps_in_bundle = [a for a in apps_in_bundle if a["name"] in selected_apps]
        if not apps_in_bundle:
            raise RuntimeError("No apps found in bundle (or none match selection)")

        app_names = [a["name"] for a in apps_in_bundle]
        for n in app_names:
            if not validate_name(n):
                raise RuntimeError(f"Invalid app name in manifest: {n!r}")

        source = manifest.get("source_instance", "unknown")
        source_platform = manifest.get("source_platform", "unknown")
        _log(f"Bundle from: {source} (platform: {source_platform})")
        _log(f"Apps to import: {', '.join(app_names)}")

        bundle_base = f"{remote_name}:{remote_path}/migrations/{bundle_name}"

        if is_remote:
            await _import_to_remote_instance(
                bundle_base,
                manifest,
                apps_in_bundle,
                app_names,
                target_url,
                target_token or "",
                all_app_data,
                rclone_conf,
            )
        else:
            await _import_to_local_filesystem(
                bundle_base,
                manifest,
                app_names,
                all_app_data,
                rclone_conf,
                config_dir,
            )

        _log("Migration import complete!")
        status = {"phase": "done", "progress": 100, "bundle": bundle_name}
        return True

    except Exception as e:
        _log(f"Import failed: {e}")
        status = {"phase": "error", "progress": 0, "error": str(e)}
        return False
    finally:
        lock.release(OpKind.MIGRATION)


# ---------------------------------------------------------------------------
# Import strategies
# ---------------------------------------------------------------------------


async def _import_to_local_filesystem(
    bundle_base: str,
    manifest: dict,
    app_names: list[str],
    all_app_data: Path,
    rclone_conf: Path,
    config_dir: Path,
) -> None:
    global status

    _log("Restoring app data to local filesystem...")
    status = {"phase": "restoring_data", "progress": 15}

    await _restore_app_data(
        bundle_base,
        app_names,
        all_app_data,
        rclone_conf,
        progress_base=15,
        progress_span=65,
    )

    bundle_checksums: dict[str, str] = manifest.get("checksums", {})
    errors = await _verify_checksums(bundle_checksums, app_names, all_app_data)
    if errors:
        _log(f"WARNING: {errors} checksum issue(s) detected")

    # Report which apps were running on the source
    apps_in_bundle = manifest.get("apps", [])
    running_apps = [
        a["name"]
        for a in apps_in_bundle
        if a.get("status") == "running" and a["name"] in app_names
    ]
    if running_apps:
        _log(f"Apps that were running on source: {', '.join(running_apps)}")
        _log(
            "Note: local import restores data only; use remote import or "
            "direct push to automatically start apps on a target instance"
        )

    # Audit receipt
    try:
        receipt = {
            "imported_at": datetime.now(timezone.utc).isoformat(),
            "bundle": bundle_base.rsplit("/", 1)[-1],
            "source_instance": manifest.get("source_instance"),
            "source_platform": manifest.get("source_platform"),
            "apps": app_names,
            "running_apps": running_apps,
        }
        receipt_dir = config_dir / "migration_receipts"
        receipt_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        (receipt_dir / f"import-{ts}.json").write_text(json.dumps(receipt, indent=2))
        _log(f"Wrote migration receipt")
    except OSError as e:
        _log(f"Warning: could not write migration receipt: {e}")


async def _import_to_remote_instance(
    bundle_base: str,
    manifest: dict,
    apps_in_bundle: list[dict],
    app_names: list[str],
    target_url: str,
    target_token: str,
    all_app_data: Path,
    rclone_conf: Path,
) -> None:
    """Deploy apps via router API and restore data to shared storage."""
    global status

    _log("Checking existing apps on target instance...")
    status = {"phase": "checking_target", "progress": 10}

    try:
        existing = await _router_get(
            "/api/apps", token=target_token, base_url=target_url
        )
    except Exception as e:
        raise RuntimeError(f"Cannot reach target instance: {e}")

    existing_names = set(existing.keys()) if isinstance(existing, dict) else set()

    # Deploy new apps
    status = {"phase": "deploying_apps", "progress": 15}
    apps_to_deploy = [
        a
        for a in apps_in_bundle
        if a["name"] not in existing_names and a.get("repo_url")
    ]

    if apps_to_deploy:
        _log(f"Deploying {len(apps_to_deploy)} new apps on target...")
        for i, app_info in enumerate(apps_to_deploy):
            app_name = app_info["name"]
            repo_url = app_info.get("repo_url")
            if not repo_url:
                _log(f"Skipping {app_name}: no repo_url in manifest")
                continue
            _log(f"Deploying {app_name} from {_strip_url_credentials(repo_url)}...")
            try:
                await _router_post(
                    "/api/add_app",
                    data={"repo_url": repo_url, "app_name": app_name},
                    token=target_token,
                    base_url=target_url,
                )
                _log(f"Deployed {app_name} (building...)")
            except Exception as e:
                _log(f"Failed to deploy {app_name}: {e}")

            pct = 15 + int(20 * (i + 1) / len(apps_to_deploy))
            status = {
                "phase": "deploying_apps",
                "progress": pct,
                "current_app": app_name,
            }

    # Wait for builds
    if apps_to_deploy:
        _log("Waiting for newly deployed apps to build and start...")
        status = {"phase": "waiting_for_apps", "progress": 40}
        max_wait, waited, deploy_failed = 300, 0, False
        while waited < max_wait:
            await asyncio.sleep(10)
            waited += 10
            try:
                current = await _router_get(
                    "/api/apps", token=target_token, base_url=target_url
                )
                all_ready, failed_apps = True, []
                for a in apps_to_deploy:
                    info = (
                        current.get(a["name"], {}) if isinstance(current, dict) else {}
                    )
                    s = info.get("status", "")
                    if s in ("building", "starting"):
                        all_ready = False
                    elif s in ("error", "failed", "crashed"):
                        failed_apps.append(a["name"])
                if failed_apps:
                    _log(f"Apps failed to deploy: {', '.join(failed_apps)}")
                    deploy_failed = True
                    break
                if all_ready:
                    break
            except Exception as e:
                _log(f"Poll error while waiting for apps: {e}")
        if waited >= max_wait:
            _log(f"Timed out waiting for apps after {waited}s")
        elif not deploy_failed:
            _log(f"Apps ready after ~{waited}s")

    # Stop apps
    _log("Stopping apps on target before data restore...")
    status = {"phase": "stopping_apps", "progress": 45}
    for app_name in app_names:
        try:
            await _router_post(
                f"/stop_app/{app_name}", token=target_token, base_url=target_url
            )
            _log(f"Stopped {app_name}")
        except Exception as e:
            _log(f"Could not stop {app_name}: {e}")
    await asyncio.sleep(3)

    # Restore data
    _log("Restoring app data from migration bundle...")
    status = {"phase": "restoring_data", "progress": 50}
    await _restore_app_data(
        bundle_base,
        app_names,
        all_app_data,
        rclone_conf,
        progress_base=50,
        progress_span=30,
    )

    # Verify
    bundle_checksums: dict[str, str] = manifest.get("checksums", {})
    errors = await _verify_checksums(bundle_checksums, app_names, all_app_data)
    if errors:
        _log(f"WARNING: {errors} checksum issue(s) detected")

    # Determine which apps should be started (those that were running on source)
    apps_to_start = [
        a["name"]
        for a in apps_in_bundle
        if a.get("status") == "running" and a["name"] in app_names
    ]
    apps_to_leave_stopped = [n for n in app_names if n not in apps_to_start]

    if apps_to_leave_stopped:
        _log(
            f"Apps that were not running on source (will remain stopped): "
            f"{', '.join(apps_to_leave_stopped)}"
        )

    # Restart only previously-running apps
    if apps_to_start:
        _log(
            f"Restarting previously-running apps on target: {', '.join(apps_to_start)}"
        )
        status = {"phase": "restarting_apps", "progress": 90}
        for app_name in apps_to_start:
            try:
                await _router_post(
                    f"/reload_app/{app_name}", token=target_token, base_url=target_url
                )
                _log(f"Restarted {app_name}")
            except Exception as e:
                _log(f"Could not restart {app_name}: {e}")
    else:
        _log("No apps were running on source; all apps left stopped on target")

    # Final verification
    _log("Verifying apps on target...")
    status = {"phase": "verifying", "progress": 95}
    await asyncio.sleep(10)
    try:
        final = await _router_get("/api/apps", token=target_token, base_url=target_url)
        if not isinstance(final, dict):
            _log("Unexpected response format from target")
            return
        for app_name in app_names:
            s = final.get(app_name, {}).get("status", "unknown")
            err = final.get(app_name, {}).get("error_message", "")
            expected = "running" if app_name in apps_to_start else "stopped"
            status_note = f" (expected: {expected})" if s != expected else ""
            _log(
                f"{app_name}: {s}{status_note}"
                + (f" ({err})" if err and s not in ("running", "stopped") else "")
            )
    except Exception as e:
        _log(f"Could not verify final status: {e}")


# ---------------------------------------------------------------------------
# Bundle management
# ---------------------------------------------------------------------------


async def list_bundles(
    rclone_conf: Path, load_config: callable
) -> tuple[list[str], bool]:
    conf = load_config()
    remote_name = conf["remote_name"]
    remote_path = conf["remote_path"]

    if not rclone_conf.exists() or not remote_name or not remote_path:
        return [], False

    try:
        proc = await asyncio.create_subprocess_exec(
            "rclone",
            "lsjson",
            f"{remote_name}:{remote_path}/migrations",
            "--config",
            str(rclone_conf),
            "--dirs-only",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            err = stderr.decode().strip()
            if "directory not found" in err.lower() or "not found" in err.lower():
                return [], True
            logger.error("Failed to list migration bundles: %s", err)
            return [], False

        entries = json.loads(stdout.decode())
        names = sorted(
            [
                e["Path"]
                for e in entries
                if e.get("IsDir")
                and e["Path"].startswith(MIGRATION_DIR_PREFIX)
                and validate_name(e["Path"])
            ],
            reverse=True,
        )
        return names, True
    except Exception:
        logger.exception("Failed to list migration bundles")
        return [], False


async def get_manifest(
    bundle_name: str, rclone_conf: Path, load_config: callable
) -> dict | None:
    conf = load_config()
    remote_name = conf["remote_name"]
    remote_path = conf["remote_path"]

    with tempfile.TemporaryDirectory() as tmpdir:
        src = f"{remote_name}:{remote_path}/migrations/{bundle_name}/manifest.json"
        dst = Path(tmpdir) / "manifest.json"
        proc = await asyncio.create_subprocess_exec(
            "rclone",
            "copyto",
            src,
            str(dst),
            "--config",
            str(rclone_conf),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0 or not dst.exists():
            err = stderr.decode().strip() if stderr else ""
            if err:
                logger.warning("Failed to download manifest: %s", err)
            return None
        try:
            result = json.loads(dst.read_text())
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning("Failed to parse manifest.json: %s", e)
            return None
        if not isinstance(result, dict):
            logger.warning("manifest.json is not a JSON object")
            return None
        return result


async def delete_bundle(
    bundle_name: str, rclone_conf: Path, load_config: callable
) -> bool:
    conf = load_config()
    remote_name = conf["remote_name"]
    remote_path = conf["remote_path"]

    if not rclone_conf.exists():
        return False

    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            "rclone",
            "purge",
            f"{remote_name}:{remote_path}/migrations/{bundle_name}",
            "--config",
            str(rclone_conf),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
        if proc.returncode == 0:
            _log(f"Deleted migration bundle: {bundle_name}")
            return True
        else:
            _log(f"Failed to delete bundle: {stderr.decode().strip()}")
            return False
    except asyncio.TimeoutError:
        if proc is not None:
            proc.kill()
            await proc.wait()
        _log(f"Delete timed out for bundle: {bundle_name}")
        return False
    except Exception as e:
        _log(f"Delete failed: {e}")
        return False


# ===================================================================
# Direct push migration  (no shared storage required)
# ===================================================================
#
# Source (this instance) streams app data directly to the target
# instance's backup app over HTTP.  The protocol is:
#
#   1. POST /api/migration/receive/start   — send manifest
#   2. POST /api/migration/receive/app/:n  — stream tar.gz per app
#   3. POST /api/migration/receive/finalize — deploy apps via router
#
# The target needs the backup app running.  Both sides authenticate
# with their respective tokens.


def _tar_directory_sync(directory: Path) -> bytes:
    """Create an in-memory tar.gz of *directory*.  Runs in a thread."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        tar.add(str(directory), arcname=".")
    return buf.getvalue()


async def run_direct_push(
    *,
    target_url: str,
    target_token: str,
    selected_apps: list[str] | None,
    lock: OperationLock,
    all_app_data: Path,
    vm_data_dir: Path,
    router_url: str,
    zone_domain: str,
    router_token: str | None = None,
) -> bool:
    """Push apps + data directly from this instance to a target instance.

    This is the simplified "one-click migrate" flow.  No shared rclone
    remote is needed — data is streamed over HTTP.
    """
    global status
    log.clear()
    status = {"phase": "starting", "progress": 0}

    target_backup_url = target_url.rstrip("/") + "/backup"

    try:
        # 1. Gather local app metadata
        _log("Gathering local app metadata...")
        status = {"phase": "gathering_metadata", "progress": 5}
        apps = await get_apps_metadata(vm_data_dir, router_url, token=router_token)
        apps = [a for a in apps if a["name"] != "backup"]

        if selected_apps:
            apps = [a for a in apps if a["name"] in selected_apps]

        if not apps:
            raise RuntimeError("No apps to migrate")

        app_names = [a["name"] for a in apps]
        _log(f"Apps to migrate: {', '.join(app_names)}")

        # 2. Build and send manifest to target
        _log("Sending manifest to target...")
        status = {"phase": "sending_manifest", "progress": 10}

        manifest = _build_manifest(apps, zone_domain)

        skip_verify = _is_local_url(target_backup_url)
        async with httpx.AsyncClient(verify=not skip_verify, timeout=60) as client:
            r = await client.post(
                f"{target_backup_url}/api/migration/receive/start",
                json=manifest,
                headers={"Authorization": f"Bearer {target_token}"},
            )
            if r.status_code != 200:
                body = r.text[:500]
                raise RuntimeError(
                    f"Target rejected manifest (HTTP {r.status_code}): {body}"
                )
            start_resp = r.json()
            if not start_resp.get("ok"):
                raise RuntimeError(
                    f"Target rejected manifest: {start_resp.get('error', 'unknown')}"
                )

        accepted_apps = start_resp.get("accepted_apps", app_names)
        _log(f"Target accepted {len(accepted_apps)} apps: {', '.join(accepted_apps)}")

        # 3. Stream each app's data as tar.gz
        status = {"phase": "streaming_data", "progress": 15}
        total = len(accepted_apps)

        for i, app_name in enumerate(accepted_apps):
            app_dir = all_app_data / app_name
            if not app_dir.exists():
                _log(f"Skipping {app_name}: no local data directory")
                continue

            _log(f"Compressing and sending {app_name}...")
            status = {
                "phase": "streaming_data",
                "progress": 15 + int(65 * i / total),
                "current_app": app_name,
            }

            tar_bytes = await asyncio.to_thread(_tar_directory_sync, app_dir)
            size_mb = len(tar_bytes) / (1024 * 1024)
            _log(f"  {app_name}: {size_mb:.1f} MB compressed, uploading...")

            async with httpx.AsyncClient(verify=not skip_verify, timeout=600) as client:
                r = await client.post(
                    f"{target_backup_url}/api/migration/receive/app/{app_name}",
                    content=tar_bytes,
                    headers={
                        "Authorization": f"Bearer {target_token}",
                        "Content-Type": "application/gzip",
                    },
                )
                if r.status_code != 200:
                    body = r.text[:500]
                    _log(
                        f"  WARNING: target rejected {app_name} "
                        f"(HTTP {r.status_code}): {body}"
                    )
                else:
                    resp = r.json()
                    if resp.get("ok"):
                        _log(f"  {app_name}: received by target")
                    else:
                        _log(f"  WARNING: {app_name}: {resp.get('error', 'unknown')}")

            status = {
                "phase": "streaming_data",
                "progress": 15 + int(65 * (i + 1) / total),
                "current_app": app_name,
            }

        # 4. Tell target to finalize (deploy/restart apps)
        _log("Finalizing migration on target...")
        status = {"phase": "finalizing", "progress": 85}

        # Build a repo_url map with credentials for the target to deploy from.
        # The manifest contains stripped URLs; we need the originals for private repos.
        repo_urls = {}
        for a in apps:
            url = a.get("repo_url")
            if url and a["name"] in accepted_apps:
                repo_urls[a["name"]] = url

        async with httpx.AsyncClient(verify=not skip_verify, timeout=120) as client:
            r = await client.post(
                f"{target_backup_url}/api/migration/receive/finalize",
                json={"manifest": manifest, "repo_urls": repo_urls},
                headers={"Authorization": f"Bearer {target_token}"},
            )
            if r.status_code == 200:
                resp = r.json()
                _log(f"Target finalize: {resp.get('message', 'ok')}")
            else:
                _log(f"Target finalize returned HTTP {r.status_code}")

        _log("Migration complete!")
        status = {"phase": "done", "progress": 100}
        return True

    except Exception as e:
        _log(f"Direct push failed: {e}")
        status = {"phase": "error", "progress": 0, "error": str(e)}
        return False
    finally:
        lock.release(OpKind.MIGRATION)


# ---------------------------------------------------------------------------
# Receive endpoints (target side)
# ---------------------------------------------------------------------------
# These are called by the *source* instance during a direct push.
# They are thin enough to live here; the route wiring is in app.py.


async def receive_start(
    manifest: dict,
    all_app_data: Path,
) -> dict:
    """Validate an incoming manifest and return which apps we can accept."""
    apps = manifest.get("apps", [])
    if not apps:
        return {"ok": False, "error": "No apps in manifest"}

    accepted = []
    for app_info in apps:
        name = app_info.get("name", "")
        if not validate_name(name):
            continue
        accepted.append(name)

    if not accepted:
        return {"ok": False, "error": "No valid app names in manifest"}

    source = manifest.get("source_instance", "unknown")
    _log(f"Receive: accepted manifest from {source} with {len(accepted)} apps")
    return {"ok": True, "accepted_apps": accepted}


async def receive_app_data(
    app_name: str,
    tar_data: bytes,
    all_app_data: Path,
) -> dict:
    """Receive and extract a tar.gz of an app's data directory."""
    if not validate_name(app_name):
        return {"ok": False, "error": "Invalid app name"}

    target_dir = all_app_data / app_name
    target_dir.mkdir(parents=True, exist_ok=True)

    def _extract():
        buf = io.BytesIO(tar_data)
        with tarfile.open(fileobj=buf, mode="r:gz") as tar:
            # Security: check for path traversal in tar entries
            for member in tar.getmembers():
                member_path = Path(member.name)
                if member_path.is_absolute() or ".." in member_path.parts:
                    raise RuntimeError(
                        f"Refusing tar entry with path traversal: {member.name}"
                    )
            tar.extractall(path=str(target_dir))

    try:
        await asyncio.to_thread(_extract)
        # Fix permissions so the host router can manage this data
        await asyncio.to_thread(_fix_dir_permissions_sync, target_dir)
        size_mb = len(tar_data) / (1024 * 1024)
        _log(f"Receive: extracted {app_name} ({size_mb:.1f} MB compressed)")
        return {"ok": True}
    except Exception as e:
        _log(f"Receive: failed to extract {app_name}: {e}")
        return {"ok": False, "error": str(e)}


async def receive_finalize(
    manifest: dict,
    router_url: str,
    router_token: str | None,
    repo_urls: dict[str, str] | None = None,
) -> dict:
    """After all app data is received, deploy/restart apps via the router.

    Apps that were running on the source instance (status == "running") will
    be started on the target.  Apps that were stopped will be deployed but
    left stopped.

    ``repo_urls`` is an optional mapping of app_name -> repo_url with
    credentials intact, provided by the source during direct push.  This
    is used instead of the manifest's stripped URLs for deploying apps.
    """
    apps = manifest.get("apps", [])
    results = []

    # Determine which apps should be started after migration
    apps_to_start: set[str] = set()
    for app_info in apps:
        src_status = app_info.get("status", "")
        if src_status == "running":
            apps_to_start.add(app_info.get("name", ""))

    for app_info in apps:
        app_name = app_info.get("name", "")
        if not app_name or app_name == "backup":
            continue

        should_start = app_name in apps_to_start

        # Try to reload the app (if it already exists on this instance)
        try:
            await _router_post(
                f"/reload_app/{app_name}",
                token=router_token,
                base_url=router_url,
            )
            _log(f"Receive: reloaded {app_name}")
            results.append({"name": app_name, "action": "reloaded"})
            # If it was not running on source, stop it after reload
            if not should_start:
                try:
                    await _router_post(
                        f"/stop_app/{app_name}",
                        token=router_token,
                        base_url=router_url,
                    )
                    _log(f"Receive: stopped {app_name} (was not running on source)")
                except Exception as e:
                    _log(f"Receive: could not stop {app_name}: {e}")
            continue
        except Exception as e:
            _log(f"Receive: {app_name} not found on target, will deploy ({e})")
            pass  # app doesn't exist yet, try deploying

        # Try to deploy the app from its repo URL.
        # Prefer the authenticated URL from repo_urls (direct push) over
        # the stripped URL in the manifest.
        repo_url = (repo_urls or {}).get(app_name) or app_info.get("repo_url")
        if repo_url:
            try:
                await _router_post(
                    "/api/add_app",
                    data={"repo_url": repo_url, "app_name": app_name},
                    token=router_token,
                    base_url=router_url,
                )
                _log(
                    f"Receive: deployed {app_name} from {_strip_url_credentials(repo_url)}"
                )
                results.append(
                    {
                        "name": app_name,
                        "action": "deployed",
                        "should_start": should_start,
                    }
                )
            except Exception as e:
                _log(f"Receive: could not deploy {app_name}: {e}")
                results.append({"name": app_name, "action": "failed", "error": str(e)})
        else:
            _log(f"Receive: {app_name} data received but no repo_url to deploy from")
            results.append({"name": app_name, "action": "data_only"})

    # Stop apps that were not running on the source but were deployed/reloaded.
    # Newly deployed apps need time to build first — wait for them, then stop.
    apps_to_stop = [
        r["name"]
        for r in results
        if r["name"] not in apps_to_start
        and r.get("action") in ("deployed", "reloaded")
    ]

    if apps_to_stop:
        # Wait for deployed apps to finish building before stopping
        deployed_to_stop = [
            r["name"]
            for r in results
            if r["name"] in apps_to_stop and r.get("action") == "deployed"
        ]
        if deployed_to_stop:
            _log(
                f"Waiting for apps to build before stopping: {', '.join(deployed_to_stop)}"
            )
            max_wait, waited = 300, 0
            while waited < max_wait:
                await asyncio.sleep(10)
                waited += 10
                try:
                    current = await _router_get(
                        "/api/apps", token=router_token, base_url=router_url
                    )
                    all_done = True
                    for name in deployed_to_stop:
                        info = (
                            current.get(name, {}) if isinstance(current, dict) else {}
                        )
                        s = info.get("status", "")
                        if s in ("building", "starting"):
                            all_done = False
                            break
                    if all_done:
                        break
                except Exception as e:
                    _log(f"Receive: poll error while waiting for builds: {e}")
            if waited >= max_wait:
                _log(f"Timed out waiting for apps to build after {waited}s")

        for app_name in apps_to_stop:
            try:
                await _router_post(
                    f"/stop_app/{app_name}",
                    token=router_token,
                    base_url=router_url,
                )
                _log(f"Receive: stopped {app_name} (was not running on source)")
            except Exception as e:
                _log(f"Receive: could not stop {app_name}: {e}")

    return {
        "ok": True,
        "message": f"Finalized {len(results)} apps",
        "results": results,
        "apps_to_start": sorted(apps_to_start),
    }
