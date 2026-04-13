"""Cross-instance migration via direct push.

This module handles migrating apps and data between OpenHost instances.
The source streams app data as tar.gz archives directly to the target
instance's backup app over HTTP.  The target stops its apps, wipes data
for migrated apps, receives the new data, then deploys/restarts apps.

The protocol is:
  1. POST /api/migration/receive/start   -- send manifest, target stops apps + wipes data
  2. POST /api/migration/receive/app/:n  -- stream tar.gz per app
  3. POST /api/migration/receive/finalize -- deploy/restart apps via router API
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import re
import shutil
import sqlite3
import tarfile
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import httpx

from operations import OpKind, OperationLock

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MIGRATION_PROTOCOL_VERSION = 3
# Allow alphanumerics, hyphens, dots, underscores, and colons (for timestamps)
MIGRATION_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._:T-]*$")


# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
# Progress / log state lives here rather than in app.py.  The route handlers
# expose these via the ``/api/migration/status`` endpoint.

status: dict | None = None  # {"phase": ..., "progress": ..., ...}
log: list[str] = []
# Apps that were stopped on the destination during receive_start.
# Used by receive_finalize to restart non-migrated apps afterward.
_receive_stopped_apps: list[str] = []


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def validate_name(name: str) -> bool:
    """Validate a name (app name or label) to prevent path traversal."""
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
        logger.warning("Failed to parse URL for credential stripping, omitting URL")
        return None  # Don't leak credentials on parse failure
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

        try:
            result = await asyncio.to_thread(_read)
        except Exception as e:
            logger.warning("Failed to read router.db, falling back to API: %s", e)
            result = None
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
                except Exception as e:
                    logger.warning("Could not read git remote for %s: %s", name, e)

    return apps


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------


def _fix_permissions(directory: Path) -> None:
    """Fix ownership and permissions so the host router can manage the data.

    The backup container runs as root, but the OpenHost router runs as the
    host user. After extracting tar data, files are owned by root and the
    router's provision_data() will fail with PermissionError on chmod.

    We detect the correct uid:gid by looking at the parent directory
    (which was created by the router), then chown + chmod recursively.
    """
    if not directory.exists():
        return
    import os
    import stat

    # Detect target uid:gid from the parent directory (owned by the host user)
    parent = directory.parent
    try:
        parent_stat = os.stat(str(parent))
        target_uid = parent_stat.st_uid
        target_gid = parent_stat.st_gid
    except OSError:
        target_uid = -1
        target_gid = -1

    count = 0
    for root, dirs, files in os.walk(str(directory)):
        for d in dirs:
            path = os.path.join(root, d)
            try:
                if target_uid >= 0:
                    os.chown(path, target_uid, target_gid)
                os.chmod(path, 0o777)
                count += 1
            except OSError as e:
                logger.warning("fix_permissions failed for dir %s: %s", path, e)
        for f in files:
            path = os.path.join(root, f)
            try:
                if target_uid >= 0:
                    os.chown(path, target_uid, target_gid)
                st = os.stat(path)
                os.chmod(path, st.st_mode | stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
                count += 1
            except OSError as e:
                logger.warning("fix_permissions failed for file %s: %s", path, e)
    try:
        if target_uid >= 0:
            os.chown(str(directory), target_uid, target_gid)
        os.chmod(str(directory), 0o777)
        count += 1
    except OSError as e:
        logger.warning("fix_permissions failed for root dir %s: %s", directory, e)
    logger.info(
        "Fixed permissions on %d items in %s (uid=%s gid=%s)",
        count,
        directory,
        target_uid,
        target_gid,
    )


def _build_manifest(
    apps: list[dict],
    zone_domain: str,
    checksums: dict[str, str] | None = None,
) -> dict:
    return {
        "version": MIGRATION_PROTOCOL_VERSION,
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
                raise RuntimeError(
                    f"Target finalize failed (HTTP {r.status_code}): {r.text[:200]}"
                )

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
    router_url: str = "",
    router_token: str | None = None,
) -> dict:
    """Validate an incoming manifest, stop destination apps, and clean data.

    1. Validate the manifest and determine accepted apps
    2. Stop ALL non-backup apps on this instance (so nothing holds
       file handles during the data wipe/restore)
    3. Delete app data directories for apps that will be migrated
       (clean slate — no leftover hybrid state)
    """
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

    # --- Stop all non-backup apps on this instance ---
    stopped_apps: list[str] = []
    if router_url and router_token:
        _log("Receive: stopping all apps on destination before data transfer...")
        try:
            existing = await _router_get(
                "/api/apps", token=router_token, base_url=router_url
            )
            if isinstance(existing, dict):
                for app_name, info in existing.items():
                    if app_name == "backup":
                        continue
                    if info.get("status") in ("running", "building", "starting"):
                        try:
                            await _router_post(
                                f"/stop_app/{app_name}",
                                token=router_token,
                                base_url=router_url,
                            )
                            stopped_apps.append(app_name)
                            _log(f"Receive: stopped {app_name}")
                        except Exception as e:
                            _log(f"Receive: could not stop {app_name}: {e}")
        except Exception as e:
            _log(f"Receive: could not list apps to stop: {e}")

        # Give containers a moment to fully stop
        if stopped_apps:
            await asyncio.sleep(3)

    # --- Delete app data for migrated apps (clean slate) ---
    for app_name in accepted:
        app_dir = all_app_data / app_name
        if app_dir.exists():
            _log(f"Receive: deleting existing data for {app_name}")
            try:
                await asyncio.to_thread(shutil.rmtree, app_dir)
            except Exception as e:
                _log(f"Receive: could not fully delete {app_name} data: {e}")
                # Try to at least empty it
                try:
                    for child in app_dir.iterdir():
                        if child.is_dir():
                            await asyncio.to_thread(shutil.rmtree, child)
                        else:
                            child.unlink()
                except Exception as e2:
                    _log(f"Receive: fallback cleanup also failed for {app_name}: {e2}")

    # Store stopped apps so receive_finalize can restart non-migrated ones
    global _receive_stopped_apps
    _receive_stopped_apps = stopped_apps

    _log(f"Receive: ready for data transfer ({len(accepted)} apps)")
    return {
        "ok": True,
        "accepted_apps": accepted,
        "stopped_apps": stopped_apps,
    }


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
            # Custom filter: block path traversal but allow symlinks.
            # The built-in filter='data' rejects symlinks to absolute paths,
            # which breaks apps that use symlinks in their data directories
            # (e.g. agent-host creates symlinks to shared config locations).
            def _migration_filter(member, dest_path):
                # Block path traversal
                if ".." in member.name.split("/"):
                    return None
                # Block absolute paths in member names
                if member.name.startswith("/"):
                    return None
                return member

            tar.extractall(path=str(target_dir), filter=_migration_filter)

    try:
        await asyncio.to_thread(_extract)
        await asyncio.to_thread(_fix_permissions, target_dir)
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

    # --- Restart non-migrated apps that were stopped during receive_start ---
    global _receive_stopped_apps
    migrated_names = {a.get("name", "") for a in apps if a.get("name")}
    non_migrated_stopped = [
        name
        for name in _receive_stopped_apps
        if name not in migrated_names and name != "backup"
    ]
    if non_migrated_stopped:
        _log(
            f"Receive: restarting non-migrated apps that were stopped: "
            f"{', '.join(non_migrated_stopped)}"
        )
        for app_name in non_migrated_stopped:
            try:
                await _router_post(
                    f"/reload_app/{app_name}",
                    token=router_token,
                    base_url=router_url,
                )
                _log(f"Receive: restarted {app_name}")
            except Exception as e:
                _log(f"Receive: could not restart {app_name}: {e}")

    # Clear the receive state
    _receive_stopped_apps = []

    failed = [r for r in results if r.get("action") == "failed"]
    all_failed = len(failed) == len(results) and results
    return {
        "ok": not all_failed,
        "message": f"Finalized {len(results)} apps"
        + (f" ({len(failed)} failed)" if failed else ""),
        "results": results,
        "apps_to_start": sorted(apps_to_start),
    }
