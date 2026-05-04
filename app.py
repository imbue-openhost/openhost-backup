import asyncio
import json
import logging
import os
import re
import secrets
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from quart import Quart, render_template, request, jsonify

import migration
from operations import OpKind, OperationLock

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
logger.info("backup app module loaded")

app = Quart(__name__)
# Allow large request bodies for migration data transfers.
# Set to 10GB to effectively disable the limit.
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024 * 1024  # 10 GB

# ---------------------------------------------------------------------------
# Paths & configuration
# ---------------------------------------------------------------------------

BASE_PATH = os.environ.get("OPENHOST_APP_BASE_PATH", "/backup")
APP_DATA_DIR = Path(os.environ.get("OPENHOST_APP_DATA_DIR", "/data/app_data/backup"))
ALL_APP_DATA = Path("/data/app_data")
APP_TEMP_DATA = Path("/data/app_temp_data")
APP_ARCHIVE = Path("/data/app_archive")
VM_DATA_DIR = Path("/data/vm_data")

# Roots the backup app captures when the ``access_all_data = true``
# manifest permission is in effect. Order is significant only for UI
# display (``list_snapshot_files`` surfaces these as the top-level
# entries when ``root`` is unset). Any root that doesn't exist on disk
# at backup time is skipped silently so the app still works on
# instances that only grant a subset of these mounts.
BACKUP_ROOTS = (ALL_APP_DATA, APP_TEMP_DATA, VM_DATA_DIR)

# ``access_all_data = true`` mounts ``/data/app_archive`` into the
# container so the backup app can see it for migration / inspection,
# but the archive tier is intentionally NOT backed up:
#
# - ``local`` archive backend: the data already lives on the host's
#   persistent volume — operators back that up out-of-band the same
#   way they back up app_data.
# - ``s3`` archive backend: the bytes are already in S3 (the bucket
#   IS the durable store) and JuiceFS writes hourly metadata dumps
#   to ``<bucket>/<prefix>/meta/`` so the metadata DB is recoverable
#   too.  Pulling those bytes back through restic would double-store
#   them and inflate snapshot size by orders of magnitude.
#
# Restic still receives this as an explicit ``--exclude`` (in addition
# to ``/data/app_archive`` not being in BACKUP_ROOTS), so a future
# refactor that adds it to the roots list won't silently start
# capturing the archive.
BACKUP_EXCLUDES = (ALL_APP_DATA / "backup", APP_ARCHIVE)
ROUTER_URL = os.environ.get("OPENHOST_ROUTER_URL", "http://host.docker.internal:8080")
ZONE_DOMAIN = os.environ.get("OPENHOST_ZONE_DOMAIN", "")
# Router API token — the backup app needs this to call the local router API.
# The OPENHOST_APP_TOKEN is for cross-app service communication and does NOT
# grant access to router management endpoints (/api/apps, /reload_app, etc.).
# This can be set in config.json as "router_api_token" or via environment.
ROUTER_API_TOKEN = os.environ.get("OPENHOST_ROUTER_API_TOKEN", "")

CONFIG_DIR = APP_DATA_DIR
CONFIG_FILE = CONFIG_DIR / "config.json"
DB_FILE = CONFIG_DIR / "backups.db"
# Restic repository lives inside the backup app's data dir by default. This
# path is excluded from backups (see `--exclude` in run_backup).
RESTIC_REPO_DIR = APP_DATA_DIR / "restic-repo"

DEFAULT_CONFIG = {
    "interval_seconds": 3600,
    "repo": str(RESTIC_REPO_DIR),
    # Password for the restic repo. Generated on first boot if missing.
    "repo_password": "",
    # Extra env vars forwarded to restic (e.g. AWS_ACCESS_KEY_ID,
    # AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, B2_ACCOUNT_ID, etc.).
    # This is where backend credentials live.
    "env": {},
}

# Env vars that restic / its backends recognise. We whitelist what we accept
# from the API to avoid letting callers stuff arbitrary variables into the
# subprocess environment.
ALLOWED_ENV_KEYS = {
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_DEFAULT_REGION",
    "AWS_REGION",
    "B2_ACCOUNT_ID",
    "B2_ACCOUNT_KEY",
    "AZURE_ACCOUNT_NAME",
    "AZURE_ACCOUNT_KEY",
    "AZURE_ACCOUNT_SAS",
    "GOOGLE_PROJECT_ID",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "ST_AUTH",
    "ST_USER",
    "ST_KEY",
    "OS_AUTH_URL",
    "OS_REGION_NAME",
    "OS_USERNAME",
    "OS_PASSWORD",
    "OS_TENANT_ID",
    "OS_TENANT_NAME",
}

# Snapshot IDs are hex strings; restic emits 8-char short IDs and 64-char long
# ones. Accept either (plus anything in between) for validation on API input.
SNAPSHOT_ID_RE = re.compile(r"^[a-f0-9]{8,64}$")


def classify_repo(repo: str) -> dict:
    """Return ``{"type": <label>, "remote": bool, "location": <display>}``.

    Used by the UI so users can tell at a glance whether their snapshots are
    stored on a remote backend (e.g. S3) or just on the same instance's local
    disk. The latter is risky: if the instance dies, so does the backup.
    """
    if not repo:
        return {"type": "unknown", "remote": False, "location": ""}
    # Restic backend prefixes (see https://restic.readthedocs.io/en/stable/030_preparing_a_new_repo.html)
    if repo.startswith("s3:"):
        return {"type": "s3", "remote": True, "location": repo[3:]}
    if repo.startswith("b2:"):
        return {"type": "b2", "remote": True, "location": repo[3:]}
    if repo.startswith("azure:"):
        return {"type": "azure", "remote": True, "location": repo[6:]}
    if repo.startswith("gs:"):
        return {"type": "gcs", "remote": True, "location": repo[3:]}
    if repo.startswith("swift:"):
        return {"type": "swift", "remote": True, "location": repo[6:]}
    if repo.startswith("sftp:"):
        return {"type": "sftp", "remote": True, "location": repo[5:]}
    if repo.startswith("rest:"):
        return {"type": "rest-server", "remote": True, "location": repo[5:]}
    if repo.startswith("rclone:"):
        return {"type": "rclone", "remote": True, "location": repo[7:]}
    # Anything else is a local path (no scheme, or `local:`).
    if repo.startswith("local:"):
        repo_path = repo[6:]
    else:
        repo_path = repo
    return {"type": "local", "remote": False, "location": repo_path}

# Single lock for mutual exclusion across backup / restore / migration.
op_lock = OperationLock()

# Restore-specific status (not part of the lock itself).
restore_last_snapshot = None
restore_last_status = None

# Most recent `restic check` result, surfaced via /api/check/status.
check_last_status = None
check_last_output = None
check_last_at = None
check_running = False

scheduler_task = None


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


def init_db():
    """Create tables if they don't exist.  Call once at startup."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_FILE))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS backups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            status TEXT NOT NULL,
            error_message TEXT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            snapshot_id TEXT,
            data_added_bytes INTEGER,
            total_size_bytes INTEGER,
            file_count INTEGER,
            name TEXT
        )
    """)
    # Incremental schema upgrades (for installs where the old table existed).
    cursor = conn.execute("PRAGMA table_info(backups)")
    columns = {row[1] for row in cursor.fetchall()}
    for col, ddl in [
        ("snapshot_id", "ALTER TABLE backups ADD COLUMN snapshot_id TEXT"),
        ("data_added_bytes", "ALTER TABLE backups ADD COLUMN data_added_bytes INTEGER"),
        ("total_size_bytes", "ALTER TABLE backups ADD COLUMN total_size_bytes INTEGER"),
        ("file_count", "ALTER TABLE backups ADD COLUMN file_count INTEGER"),
        ("name", "ALTER TABLE backups ADD COLUMN name TEXT"),
    ]:
        if col not in columns:
            conn.execute(ddl)
    conn.commit()
    conn.close()


def get_db():
    """Get a database connection."""
    return sqlite3.connect(str(DB_FILE))


def record_backup(
    timestamp,
    status,
    error_message=None,
    snapshot_id=None,
    data_added_bytes=None,
    total_size_bytes=None,
    file_count=None,
    name=None,
):
    """Insert a backup record into the database."""
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO backups (timestamp, status, error_message, snapshot_id, "
            "data_added_bytes, total_size_bytes, file_count, name) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                timestamp,
                status,
                error_message,
                snapshot_id,
                data_added_bytes,
                total_size_bytes,
                file_count,
                name,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_last_backup():
    """Return the most recent backup record, or None."""
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT timestamp, status, error_message FROM backups ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row:
            return {"timestamp": row[0], "status": row[1], "error_message": row[2]}
        return None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------


def load_config():
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            saved = json.load(f)
        return {**DEFAULT_CONFIG, **saved}
    return dict(DEFAULT_CONFIG)


def save_config(conf):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        json.dump(conf, f, indent=2)
    # The password lives in this file so restrict perms.
    try:
        os.chmod(CONFIG_FILE, 0o600)
    except OSError:
        pass


def get_router_api_token():
    """Get the router API token from config or environment.

    Priority: config.json > OPENHOST_ROUTER_API_TOKEN env var.
    """
    conf = load_config()
    token = conf.get("router_api_token", "")
    if token:
        return token
    return ROUTER_API_TOKEN


def _extract_bearer_token() -> str | None:
    """Extract the Bearer token from the current request's Authorization header."""
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:]
    return None


async def _verify_admin_token(supplied: str | None) -> bool:
    """Return True iff ``supplied`` is a valid admin Bearer token.

    The backup app is reachable unauthenticated from inside the container
    network (co-located apps on the Docker bridge can hit
    ``http://backup:8080/...`` directly, bypassing the OpenHost router's
    auth layer). Sensitive operations — password reveal, writing the
    stored router_api_token or repo_password — must therefore require an
    explicit caller token.

    We accept any token that the local OpenHost router accepts. The
    router validates the token by checking it against the owner API
    tokens table, so this gives us real auth even though the backup app
    itself doesn't have a user database.
    """
    if not supplied:
        return False
    try:
        import httpx

        async with httpx.AsyncClient(verify=False, timeout=5) as client:
            r = await client.get(
                f"{ROUTER_URL}/api/apps",
                headers={"Authorization": f"Bearer {supplied}"},
            )
            return (
                r.status_code == 200
                and "json" in r.headers.get("content-type", "")
            )
    except Exception:
        logger.exception("Admin token verification failed")
        return False


# ---------------------------------------------------------------------------
# Restic helpers
# ---------------------------------------------------------------------------


def _restic_env(conf: dict) -> dict:
    """Environment for invoking the restic binary with repo + password set."""
    env = os.environ.copy()
    env["RESTIC_REPOSITORY"] = conf["repo"]
    env["RESTIC_PASSWORD"] = conf.get("repo_password", "")
    # Suppress progress output in unattended runs; JSON flag gives structured
    # output where we need it.
    env["RESTIC_PROGRESS_FPS"] = "0"
    # Forward any configured backend credentials (S3 keys, etc.). Only keys
    # in ALLOWED_ENV_KEYS are accepted via the API; anything already in
    # config is trusted.
    for k, v in (conf.get("env") or {}).items():
        if v is None or v == "":
            continue
        env[k] = str(v)
    return env


async def _run_restic(args: list[str], conf: dict, timeout: float | None = None):
    """Run `restic <args>` with configured repo, return (returncode, stdout, stderr).

    Raises asyncio.TimeoutError if the subprocess exceeds ``timeout``. On
    either timeout OR task cancellation, the subprocess is killed so we
    don't leak a live restic process holding the repo lock.
    """
    env = _restic_env(conf)
    proc = await asyncio.create_subprocess_exec(
        "restic",
        *args,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        # Reap the child so it doesn't become a zombie.
        try:
            await proc.wait()
        except Exception:
            pass
        raise
    return proc.returncode, stdout, stderr


def _parse_ndjson(data: bytes):
    """Iterate over NDJSON messages in ``data``, skipping blank/invalid lines."""
    for raw in data.decode(errors="replace").splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            yield json.loads(raw)
        except json.JSONDecodeError:
            logger.debug("restic: non-JSON stdout line: %s", raw)


# Long enough for multi-GB S3 uploads on slow links but still finite — a
# wedged TCP connection can't permanently brick the scheduler.
BACKUP_TIMEOUT_SECONDS = 6 * 60 * 60  # 6 hours
RESTORE_TIMEOUT_SECONDS = 12 * 60 * 60  # 12 hours
CHECK_TIMEOUT_SECONDS = 2 * 60 * 60  # 2 hours


async def ensure_repo_initialized(
    conf: dict, *, auto_init: bool | None = None
) -> tuple[bool, str | None]:
    """Ensure the restic repo exists; run `restic init` if not.

    Returns (initialized_now, error_message).

    ``auto_init`` controls what happens when ``cat config`` fails with a
    "repo does not exist" signal:

    - ``True``  — always run ``restic init`` (used by ``run_backup`` so the
      first scheduled backup creates the repo regardless of backend).
    - ``False`` — never auto-init; report a "not initialized" error so the
      caller / UI can prompt the user explicitly.
    - ``None``  — auto-init only when the repo is local (no remote backend
      prefix).  Safer default for read-only operations: a typo'd S3 URL
      won't silently create an empty bucket-side repo at the wrong path,
      but a fresh local install still "just works" when the user clicks
      a UI button.
    """
    # `cat config` is a cheap way to confirm the repo exists and the password
    # is correct. It returns non-zero on either missing repo or wrong password.
    rc, _stdout, stderr = await _run_restic(["cat", "config"], conf, timeout=30)
    if rc == 0:
        return False, None

    err = stderr.decode(errors="replace").strip()
    # If the repo simply doesn't exist, decide whether to init. Heuristic on
    # the error text; restic doesn't expose a clean "not found" exit code.
    err_lower = err.lower()
    is_not_initialized = (
        "does not exist" in err_lower
        or "unable to open config" in err_lower
        or "no such file" in err_lower
    )
    if is_not_initialized:
        info = classify_repo(conf["repo"])
        should_init = auto_init if auto_init is not None else not info["remote"]
        if not should_init:
            return False, (
                f"Repository not initialized at {conf['repo']!r}. Run a backup "
                f"to create it, or pass auto_init=True for this operation."
            )
        # Local repo path: make sure parent exists. classify_repo already
        # strips any `local:` prefix, so we use its `location` as the on-disk
        # path rather than the raw repo string.
        if info["type"] == "local" and info["location"]:
            Path(info["location"]).parent.mkdir(parents=True, exist_ok=True)
        rc2, _out2, err2 = await _run_restic(["init"], conf, timeout=60)
        if rc2 != 0:
            return False, f"restic init failed: {err2.decode(errors='replace').strip()}"
        return True, None
    return False, f"restic repo check failed: {err}"


async def _restic_unlock_if_stale(conf: dict) -> None:
    """Best-effort remove a stale repo lock at startup."""
    try:
        await _run_restic(["unlock"], conf, timeout=30)
    except Exception:
        logger.warning("restic unlock failed on startup", exc_info=True)


# ---------------------------------------------------------------------------
# Backup
# ---------------------------------------------------------------------------


async def run_backup(name: str | None = None) -> bool:
    err = op_lock.try_acquire(OpKind.BACKUP)
    if err:
        logger.warning("Skipping backup: %s", err)
        return False

    try:
        conf = load_config()
    except Exception:
        logger.exception("Failed to load backup config")
        op_lock.release(OpKind.BACKUP)
        return False

    if not conf.get("repo") or not conf.get("repo_password"):
        logger.error("Restic repo or password not configured, skipping backup")
        op_lock.release(OpKind.BACKUP)
        return False

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    logger.info("Starting restic backup to %s", conf["repo"])

    try:
        # Backup always creates the repo if missing — that's the operation
        # users opt into knowing it'll write to the configured location.
        init_err = (await ensure_repo_initialized(conf, auto_init=True))[1]
        if init_err:
            record_backup(timestamp, "error", init_err, name=name)
            logger.error("Backup failed: %s", init_err)
            return False

        tags = ["openhost"]
        if name:
            tags.append(f"name:{name}")

        # Back up every mounted root (app_data, app_temp_data, vm_data).
        # Skip ones that aren't present — this keeps the app usable on
        # instances that only grant a subset of data permissions.
        roots = [p for p in BACKUP_ROOTS if p.is_dir()]
        if not roots:
            msg = (
                "No backup roots available — expected one of: "
                + ", ".join(str(p) for p in BACKUP_ROOTS)
            )
            record_backup(timestamp, "error", msg, name=name)
            logger.error(msg)
            return False

        args = ["backup", "--json"]
        args += [str(p) for p in roots]
        # Exclude our own restic repo (avoid self-inclusion + infinite
        # growth) and ``/data/app_archive`` (rationale documented at the
        # BACKUP_EXCLUDES definition).
        for ex in BACKUP_EXCLUDES:
            args += ["--exclude", str(ex)]
        for t in tags:
            args += ["--tag", t]

        # Go through the shared helper so the subprocess has a bounded
        # timeout and gets properly killed on cancellation. BACKUP_TIMEOUT
        # is generous for large instances but still finite — a wedged S3
        # connection would otherwise hold the op lock forever.
        try:
            rc, stdout, stderr = await _run_restic(args, conf, timeout=BACKUP_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            msg = f"restic backup timed out after {BACKUP_TIMEOUT_SECONDS}s"
            record_backup(timestamp, "error", msg, name=name)
            logger.error(msg)
            return False

        summary = None
        if stdout:
            # Restic emits NDJSON to stdout with --json; the last `summary`
            # message contains the snapshot ID and byte counts.
            for msg in _parse_ndjson(stdout):
                if msg.get("message_type") == "summary":
                    summary = msg

        if stderr:
            for line in stderr.decode(errors="replace").splitlines():
                if line.strip():
                    logger.info("restic stderr: %s", line)

        if rc == 0 and summary is not None:
            record_backup(
                timestamp,
                "success",
                snapshot_id=summary.get("snapshot_id"),
                data_added_bytes=summary.get("data_added"),
                total_size_bytes=summary.get("total_bytes_processed"),
                file_count=summary.get("total_files_processed"),
                name=name,
            )
            logger.info(
                "Backup completed: snapshot=%s data_added=%s total=%s",
                summary.get("snapshot_id", "?"),
                summary.get("data_added", "?"),
                summary.get("total_bytes_processed", "?"),
            )
            return True

        if rc == 0:
            # Succeeded but we somehow missed the summary line.
            record_backup(timestamp, "success", name=name)
            logger.info("Backup completed (no summary parsed)")
            return True

        error_msg = stderr.decode(errors="replace").strip() or f"restic exit code {rc}"
        record_backup(timestamp, "error", error_msg, name=name)
        logger.error("Backup failed: %s", error_msg)
        return False
    except Exception as e:
        record_backup(timestamp, "error", str(e), name=name)
        logger.exception("Backup failed")
        return False
    finally:
        op_lock.release(OpKind.BACKUP)


# ---------------------------------------------------------------------------
# Snapshot helpers
# ---------------------------------------------------------------------------


async def list_snapshots() -> tuple[list[dict], bool]:
    """Return (snapshots, repo_ok).

    Each snapshot entry has: {id, short_id, time, paths, tags, hostname}.
    """
    conf = load_config()
    if not conf.get("repo") or not conf.get("repo_password"):
        return [], False
    # Auto-init for local repos so the snapshots panel doesn't render
    # "unable to open config file" on a freshly-configured install where
    # the user hasn't triggered a backup yet.  Remote repos are NOT
    # auto-inited from a read endpoint — that's reserved for run_backup
    # so a typo'd S3/B2/SFTP URL can't silently create an empty repo at
    # the wrong location.
    init_err = (await ensure_repo_initialized(conf))[1]
    if init_err:
        logger.info("list_snapshots: %s", init_err)
        return [], False
    try:
        # Scope to the "openhost" tag so, if the user points this app at a
        # repo shared with other hosts/projects, we only surface snapshots
        # written by this app. Backups are created with --tag openhost in
        # run_backup.
        rc, stdout, stderr = await _run_restic(
            ["snapshots", "--json", "--tag", "openhost"], conf, timeout=60
        )
        if rc != 0:
            logger.error("restic snapshots failed: %s", stderr.decode(errors="replace").strip())
            return [], False
        entries = json.loads(stdout.decode(errors="replace") or "[]")
        out = []
        for e in entries:
            out.append(
                {
                    "id": e.get("id", ""),
                    "short_id": e.get("short_id", ""),
                    "time": e.get("time", ""),
                    "paths": e.get("paths", []),
                    "tags": e.get("tags", []) or [],
                    "hostname": e.get("hostname", ""),
                }
            )
        # Newest first
        out.sort(key=lambda x: x["time"], reverse=True)
        return out, True
    except Exception:
        logger.exception("Failed to list snapshots")
        return [], False


async def repo_stats() -> tuple[dict | None, str | None]:
    """Return (stats, error) — how much space the restic repo is using.

    Uses ``restic stats --mode raw-data`` which reports the deduplicated /
    compressed on-disk footprint of the repository (this is the number
    that matters for S3 cost / local disk usage). Also scopes to the
    openhost tag so a shared repo isn't double-counted with unrelated
    snapshots.
    """
    conf = load_config()
    if not conf.get("repo") or not conf.get("repo_password"):
        return None, "Restic repo not configured"
    # Auto-init only for local repos (see list_snapshots for the rationale).
    init_err = (await ensure_repo_initialized(conf))[1]
    if init_err:
        return None, init_err
    try:
        rc, stdout, stderr = await _run_restic(
            ["stats", "--mode", "raw-data", "--json", "--tag", "openhost"],
            conf,
            timeout=60,
        )
        if rc != 0:
            return None, stderr.decode(errors="replace").strip() or f"restic exit {rc}"
        data = json.loads(stdout.decode(errors="replace") or "{}")
        # raw-data mode returns total_size / total_blob_count / snapshots_count
        # and compression stats. It does NOT return total_file_count (that
        # only exists for restore-size / files-by-contents). We surface
        # total_size because that's the actual on-disk / S3 footprint.
        return {
            "total_size_bytes": data.get("total_size", 0),
            "total_uncompressed_size_bytes": data.get("total_uncompressed_size", 0),
            "total_blob_count": data.get("total_blob_count", 0),
            "snapshots_count": data.get("snapshots_count", 0),
            "compression_ratio": data.get("compression_ratio"),
        }, None
    except Exception as e:
        logger.exception("repo_stats failed")
        return None, str(e)


def validate_subpath(path: str) -> bool:
    if not path:
        return True
    for seg in path.split("/"):
        if seg in ("..", ".") or not seg:
            return False
        if not re.match(r"^[\w\-:. ]+$", seg):
            return False
    return True


_ROOT_NAMES = {
    "app_data": ALL_APP_DATA,
    "app_temp_data": APP_TEMP_DATA,
    "vm_data": VM_DATA_DIR,
}


async def _list_roots_in_snapshot(snapshot_id: str, conf: dict):
    """Return the list of BACKUP_ROOTS actually present in this snapshot.

    A snapshot only contains roots that existed on disk at backup time,
    so we probe each one with ``restic ls`` to figure out which to show
    as top-level entries in the browser.
    """
    present: list[dict] = []
    for name, path in _ROOT_NAMES.items():
        args = ["ls", "--json", snapshot_id, str(path)]
        try:
            rc, _stdout, _stderr = await _run_restic(args, conf, timeout=60)
        except Exception:
            continue
        if rc == 0:
            present.append(
                {
                    "path": name,
                    "size": 0,
                    "is_dir": True,
                    "mod_time": "",
                }
            )
    return present


async def list_snapshot_files(
    snapshot_id: str, subpath: str = "", root: str | None = None
):
    """List files in a snapshot.

    Browsing model:
      * ``root`` unset → return the synthetic top level (one entry per
        captured root: app_data / app_temp_data / vm_data).
      * ``root`` set → resolve to the matching absolute path, optionally
        appended with ``subpath``, and return direct children of that dir
        from the snapshot.

    Returns ``(files, error)``.
    """
    conf = load_config()
    if not conf.get("repo") or not conf.get("repo_password"):
        return [], "Restic repo not configured"

    if not root:
        # Top level: surface which roots the snapshot actually contains.
        return await _list_roots_in_snapshot(snapshot_id, conf), None

    if root not in _ROOT_NAMES:
        return [], f"Unknown root: {root}"

    # Resolve the absolute path restic is being asked about.
    target_path = str(_ROOT_NAMES[root])
    if subpath:
        target_path = target_path.rstrip("/") + "/" + subpath

    args = ["ls", "--json", snapshot_id, target_path]
    try:
        rc, stdout, stderr = await _run_restic(args, conf, timeout=120)
    except Exception as e:
        return [], f"restic error: {e}"

    if rc != 0:
        err = stderr.decode(errors="replace").strip()
        if "not found" in err.lower() or "no matching" in err.lower():
            return [], "Snapshot or path not found"
        return [], f"restic error: {err}"

    files: list[dict] = []
    target_norm = target_path.rstrip("/")
    for raw in stdout.decode(errors="replace").splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if msg.get("struct_type") != "node":
            continue
        path = msg.get("path", "")
        # Only immediate children of target_path.
        if not path.startswith(target_norm + "/"):
            # Could also be an exact match of the target (the dir itself) — skip.
            continue
        rest = path[len(target_norm) + 1 :]
        if "/" in rest:
            continue  # nested deeper, not a direct child
        files.append(
            {
                "path": rest,
                "size": msg.get("size", 0) or 0,
                "is_dir": msg.get("type") == "dir",
                "mod_time": msg.get("mtime", ""),
            }
        )
    return files, None


async def delete_snapshot(snapshot_id: str) -> bool:
    """Remove a snapshot.

    Runs ``restic forget --prune`` so disk/object-store space is reclaimed
    immediately. Prune on a large repo can be slow (several minutes on an
    S3 repo with a lot of data) — we set a generous but bounded timeout so
    a wedged prune can't permanently hold the UI.
    """
    conf = load_config()
    if not conf.get("repo") or not conf.get("repo_password"):
        return False
    try:
        rc, _out, stderr = await _run_restic(
            ["forget", "--prune", snapshot_id], conf, timeout=30 * 60
        )
        if rc != 0:
            logger.warning(
                "restic forget failed for %s: %s",
                snapshot_id,
                stderr.decode(errors="replace").strip(),
            )
            return False
    except Exception:
        logger.exception("restic forget failed")
        return False

    # DB cleanup. Snapshot IDs stored here are always the full 64-char IDs
    # that restic emits in its --json summary, so an exact match on the
    # user-supplied ID is sufficient when they pass a full ID. When they
    # pass a short (8-char) ID, match by prefix with length >= 8 to avoid
    # accidental matches on arbitrary substrings.
    conn = get_db()
    try:
        if len(snapshot_id) >= 40:
            conn.execute(
                "DELETE FROM backups WHERE snapshot_id = ?", (snapshot_id,)
            )
        else:
            conn.execute(
                "DELETE FROM backups WHERE substr(snapshot_id, 1, ?) = ?",
                (len(snapshot_id), snapshot_id),
            )
        conn.commit()
    except sqlite3.Error:
        # The restic forget already succeeded; don't fail the operation.
        logger.exception("DB cleanup failed for snapshot %s", snapshot_id)
    finally:
        conn.close()
    logger.info("Deleted snapshot %s", snapshot_id)
    return True


def get_backup_history(limit=20, offset=0):
    conn = get_db()
    try:
        total = conn.execute("SELECT COUNT(*) FROM backups").fetchone()[0]
        rows = conn.execute(
            "SELECT id, timestamp, status, error_message, created_at, snapshot_id, "
            "data_added_bytes, total_size_bytes, file_count, name "
            "FROM backups ORDER BY id DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
        history = [
            {
                "id": r[0],
                "timestamp": r[1],
                "status": r[2],
                "error_message": r[3],
                "created_at": r[4],
                "snapshot_id": r[5],
                "data_added_bytes": r[6],
                "total_size_bytes": r[7],
                "file_count": r[8],
                "name": r[9],
            }
            for r in rows
        ]
        return history, total
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Restore
# ---------------------------------------------------------------------------


async def run_restore(snapshot_id: str, root: str | None = None) -> bool:
    """Restore a snapshot.

    If ``root`` is None, every captured path in the snapshot is
    restored. Otherwise only the named root (``app_data``,
    ``app_temp_data``, or ``vm_data``) is touched via restic's
    ``--include`` filter.
    """
    global restore_last_snapshot, restore_last_status

    err = op_lock.try_acquire(OpKind.RESTORE)
    if err:
        logger.warning("Skipping restore: %s", err)
        return False

    try:
        conf = load_config()
    except Exception:
        logger.exception("Failed to load restore config")
        op_lock.release(OpKind.RESTORE)
        return False

    if not conf.get("repo") or not conf.get("repo_password"):
        restore_last_status = "error: restic repo not configured"
        op_lock.release(OpKind.RESTORE)
        return False

    if not SNAPSHOT_ID_RE.match(snapshot_id):
        restore_last_status = "error: invalid snapshot id"
        op_lock.release(OpKind.RESTORE)
        return False

    if root is not None and root not in _ROOT_NAMES:
        restore_last_status = f"error: unknown root '{root}'"
        op_lock.release(OpKind.RESTORE)
        return False

    logger.info(
        "Starting restic restore from %s (root=%s)", snapshot_id, root or "all"
    )

    try:
        args = [
            "restore",
            snapshot_id,
            "--target",
            "/",  # restic restores the absolute paths as they were captured
        ]
        # restic 0.17 forbids mixing --include and --exclude in one
        # restore. When restoring a specific root we use --include
        # (narrowing); otherwise we use --exclude so we don't clobber
        # our own repo directory or the archive tier (which the backup
        # never captured in the first place — see BACKUP_EXCLUDES).
        if root:
            args += ["--include", str(_ROOT_NAMES[root])]
        else:
            for ex in BACKUP_EXCLUDES:
                args += ["--exclude", str(ex)]
        try:
            rc, _stdout, stderr = await _run_restic(
                args, conf, timeout=RESTORE_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            restore_last_status = (
                f"error: restore timed out after {RESTORE_TIMEOUT_SECONDS}s"
            )
            logger.error(restore_last_status)
            return False

        if rc == 0:
            restore_last_snapshot = snapshot_id
            restore_last_status = "success"
            logger.info("Restore completed successfully")
        else:
            restore_last_status = (
                f"error: {stderr.decode(errors='replace').strip() or f'restic exit {rc}'}"
            )
            logger.error("Restore failed: %s", restore_last_status)
    except Exception as e:
        restore_last_status = f"error: {e}"
        logger.exception("Restore failed")
    finally:
        op_lock.release(OpKind.RESTORE)

    return restore_last_status == "success"


# ---------------------------------------------------------------------------
# Check (repo integrity)
# ---------------------------------------------------------------------------


async def run_check() -> bool:
    """Run `restic check`. Updates module-level state.

    Note: this coroutine doesn't take ``op_lock`` itself — callers (the
    HTTP route) are expected to gate on both ``op_lock.busy`` and
    ``check_running`` to avoid colliding with backup/restore or another
    concurrent check. restic itself acquires its own repo-level lock.
    """
    global check_last_status, check_last_output, check_last_at, check_running
    # Set the flag inside the try so that any exception from load_config /
    # _run_restic still runs the finally clause that clears it. Without
    # this, a corrupt config.json would leave check_running=True forever.
    try:
        check_running = True
        conf = load_config()
        if not conf.get("repo") or not conf.get("repo_password"):
            check_last_status = "error"
            check_last_output = "Restic repo not configured"
            check_last_at = datetime.now(timezone.utc).isoformat()
            return False
        # Auto-init only for local repos so a fresh-install user clicking
        # "Run check" doesn't see a confusing "unable to open config file"
        # error on a repo that simply hasn't been backed up yet.  Remote
        # repos still error here so we don't silently create them.
        init_err = (await ensure_repo_initialized(conf))[1]
        if init_err:
            check_last_status = "error"
            check_last_output = init_err
            check_last_at = datetime.now(timezone.utc).isoformat()
            logger.info("run_check: %s", init_err)
            return False
        try:
            rc, stdout, stderr = await _run_restic(
                ["check"], conf, timeout=CHECK_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            check_last_status = "error"
            check_last_output = f"restic check timed out after {CHECK_TIMEOUT_SECONDS}s"
            check_last_at = datetime.now(timezone.utc).isoformat()
            logger.error(check_last_output)
            return False
        output = (stdout.decode(errors="replace") + stderr.decode(errors="replace")).strip()
        check_last_output = output[-4000:]  # cap
        check_last_at = datetime.now(timezone.utc).isoformat()
        if rc == 0:
            check_last_status = "ok"
            logger.info("restic check ok")
            return True
        check_last_status = "error"
        logger.error("restic check failed: %s", output)
        return False
    except Exception as e:
        check_last_status = "error"
        check_last_output = str(e)
        check_last_at = datetime.now(timezone.utc).isoformat()
        logger.exception("restic check failed")
        return False
    finally:
        check_running = False


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------


async def scheduler_loop():
    first_run = True
    while True:
        conf = load_config()
        interval = conf["interval_seconds"]

        if first_run:
            first_run = False
            last = get_last_backup()
            if last and last["timestamp"]:
                try:
                    last_dt = datetime.strptime(
                        last["timestamp"], "%Y-%m-%dT%H:%M:%S"
                    ).replace(tzinfo=timezone.utc)
                    elapsed = (datetime.now(timezone.utc) - last_dt).total_seconds()
                    wait = max(0, interval - elapsed)
                except (ValueError, TypeError):
                    wait = interval
            else:
                wait = interval
        else:
            wait = interval

        logger.info("Next backup in %d seconds", int(wait))
        await asyncio.sleep(wait)
        await run_backup()


def ensure_default_config():
    """Make sure config.json exists and has a repo password.

    Generates a random password on first run if the user hasn't supplied one.
    The password is stored in config.json (0600); losing it makes the repo
    unreadable, so surface it prominently in the UI.
    """
    conf = load_config()
    dirty = False
    if not CONFIG_FILE.exists():
        dirty = True
    if not conf.get("repo_password"):
        conf["repo_password"] = secrets.token_urlsafe(32)
        dirty = True
        logger.warning(
            "Generated new restic repo password — back it up via the Backup UI"
        )
    if not conf.get("repo"):
        conf["repo"] = str(RESTIC_REPO_DIR)
        dirty = True
    if dirty:
        save_config(conf)


@app.before_serving
async def startup():
    global scheduler_task
    init_db()
    ensure_default_config()
    # Best-effort unlock in case a previous run died mid-operation.
    try:
        conf = load_config()
        if conf.get("repo") and conf.get("repo_password"):
            await _restic_unlock_if_stale(conf)
    except Exception:
        logger.warning("startup unlock skipped", exc_info=True)
    scheduler_task = asyncio.create_task(scheduler_loop())
    logger.info("Backup scheduler started")


@app.after_serving
async def shutdown():
    if scheduler_task:
        scheduler_task.cancel()


# ---------------------------------------------------------------------------
# Route helper
# ---------------------------------------------------------------------------


def route(path, **kwargs):
    """Register a route at both /path and BASE_PATH/path to handle proxies."""

    def decorator(func):
        app.route(path, **kwargs)(func)
        if BASE_PATH and BASE_PATH != "/":
            prefixed = BASE_PATH.rstrip("/") + path
            app.route(prefixed, **kwargs)(func)
        return func

    return decorator


# ---------------------------------------------------------------------------
# Backup / restore routes
# ---------------------------------------------------------------------------


@route("/")
async def index():
    conf = load_config()
    last = get_last_backup()
    state = {
        "running": op_lock.backup_running,
        "last_backup": last["timestamp"] if last else None,
        "last_status": last["status"] if last else None,
        "last_error": last["error_message"] if last else None,
    }
    backend = classify_repo(conf.get("repo", ""))
    return await render_template(
        "index.html",
        base_path=BASE_PATH,
        config=conf,
        state=state,
        backend=backend,
    )


@route("/api/config", methods=["GET"])
async def get_config():
    conf = load_config()
    # Redact secrets: repo password, env values (backend credentials), and
    # the router API token. Report which env keys exist so the UI can
    # render them without leaking plaintext.
    env = conf.get("env") or {}
    env_keys = sorted(k for k, v in env.items() if v)
    redacted = {
        **conf,
        "repo_password": "***" if conf.get("repo_password") else "",
        "env": {k: "***" for k in env_keys},
        "env_keys": env_keys,
        "router_api_token": "***" if conf.get("router_api_token") else "",
        "backend": classify_repo(conf.get("repo", "")),
    }
    return jsonify(config=redacted)


@route("/api/config", methods=["POST"])
async def post_config():
    data = await request.get_json()
    # Writing the repo password or router_api_token is a privileged
    # operation — gate it on a valid router token so a co-located app
    # on the same Docker network can't rotate these under us. Other
    # fields (repo URL, interval, env without secrets...) are not
    # gated so the UI's default flows still work unauthenticated within
    # the router boundary.
    current_conf = load_config()
    # Writing these fields is privileged — without a check, any app on
    # the local Docker network could rotate them.
    sensitive_write = "repo_password" in data or "router_api_token" in data
    # Bootstrap exception: a fresh install has no router_api_token yet, so
    # we let the user set one without authorization. Once set, future
    # writes require an explicit Bearer token.
    bootstrap_setting_token = (
        "router_api_token" in data
        and not current_conf.get("router_api_token")
        and "repo_password" not in data
    )
    if sensitive_write and not bootstrap_setting_token:
        # Explicit Bearer token in the header only — don't fall back to the
        # app's stored router_api_token, since the point of this check is
        # to stop a local caller from rotating these secrets.
        supplied = _extract_bearer_token()
        if not await _verify_admin_token(supplied):
            return jsonify(
                ok=False,
                error="Explicit Bearer token required to modify repo_password or router_api_token",
            ), 401

    conf = current_conf
    for key in ("interval_seconds", "repo", "repo_password", "router_api_token"):
        if key in data and data[key] not in (None, "***"):
            conf[key] = data[key]
    # Handle env updates. `env` is a dict of key->value. Empty string means
    # "clear this key". Any key not in ALLOWED_ENV_KEYS is rejected so a
    # caller can't shove arbitrary vars into the subprocess env.
    if "env" in data:
        if not isinstance(data["env"], dict):
            return jsonify(ok=False, error="'env' must be an object"), 400
        current_env = dict(conf.get("env") or {})
        for k, v in data["env"].items():
            if k not in ALLOWED_ENV_KEYS:
                return jsonify(
                    ok=False,
                    error=f"env key '{k}' not allowed. Allowed: "
                    f"{', '.join(sorted(ALLOWED_ENV_KEYS))}",
                ), 400
            if v is None or v == "":
                current_env.pop(k, None)
            else:
                current_env[k] = str(v)
        conf["env"] = current_env
    if "interval_seconds" in data:
        try:
            interval = int(data["interval_seconds"])
        except (TypeError, ValueError):
            return jsonify(
                ok=False, error="interval_seconds must be an integer"
            ), 400
        conf["interval_seconds"] = max(60, interval)
    save_config(conf)
    return jsonify(ok=True)


@route("/api/config/password", methods=["GET"])
async def reveal_password():
    """Return the actual repo password.

    Gated on an explicit Bearer token in the ``Authorization`` header —
    the password unlocks the entire backup archive, and without this
    check any co-located app on the same Docker network could read it
    from ``http://backup:8080/api/config/password``.
    """
    supplied = _extract_bearer_token()
    if not await _verify_admin_token(supplied):
        return jsonify(ok=False, error="Bearer token required"), 401
    conf = load_config()
    return jsonify(ok=True, password=conf.get("repo_password", ""))


@route("/api/backup", methods=["POST"])
async def trigger_backup():
    if op_lock.busy:
        return jsonify(ok=False, error=f"{op_lock.active.value} in progress"), 409
    data = await request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip() or None
    asyncio.create_task(run_backup(name=name))
    return jsonify(ok=True, message="Backup started")


@route("/api/status")
async def status():
    conf = load_config()
    last = get_last_backup()
    return jsonify(
        running=op_lock.backup_running,
        migration_running=op_lock.migration_running,
        restore_running=op_lock.restore_running,
        last_backup=last["timestamp"] if last else None,
        last_status=last["status"] if last else None,
        last_error=last["error_message"] if last else None,
        interval_seconds=conf["interval_seconds"],
        repo=conf.get("repo", ""),
        backend=classify_repo(conf.get("repo", "")),
    )


@route("/api/snapshots")
async def api_snapshots():
    snapshots, repo_ok = await list_snapshots()
    return jsonify(ok=True, snapshots=snapshots, repo_ok=repo_ok)


@route("/api/repo/stats")
async def api_repo_stats():
    stats, error = await repo_stats()
    if error:
        return jsonify(ok=False, error=error), 500
    return jsonify(ok=True, stats=stats)


@route("/api/restore", methods=["POST"])
async def trigger_restore():
    if op_lock.busy:
        return jsonify(ok=False, error=f"{op_lock.active.value} in progress"), 409
    data = await request.get_json()
    snapshot_id = data.get("snapshot", "")
    if not snapshot_id or not SNAPSHOT_ID_RE.match(snapshot_id):
        return jsonify(ok=False, error="Invalid snapshot id"), 400
    root = data.get("root") or None
    if root is not None and root not in _ROOT_NAMES:
        return jsonify(ok=False, error=f"Unknown root: {root}"), 400
    asyncio.create_task(run_restore(snapshot_id, root=root))
    return jsonify(ok=True, message="Restore started")


@route("/api/restore/status")
async def restore_status_endpoint():
    return jsonify(
        running=op_lock.restore_running,
        last_restore=restore_last_snapshot,
        last_status=restore_last_status,
    )


@route("/api/snapshot/files")
async def snapshot_files():
    snapshot_id = request.args.get("snapshot", "")
    if not snapshot_id or not SNAPSHOT_ID_RE.match(snapshot_id):
        return jsonify(ok=False, error="Invalid snapshot id"), 400
    # ``root`` picks one of the three captured top-level trees. Omitted =
    # return the synthetic root that lists all captured trees.
    root = request.args.get("root") or None
    if root is not None and root not in _ROOT_NAMES:
        return jsonify(ok=False, error=f"Unknown root: {root}"), 400
    subpath = request.args.get("path", "")
    if not validate_subpath(subpath):
        return jsonify(ok=False, error="Invalid path"), 400
    try:
        files, error = await list_snapshot_files(snapshot_id, subpath, root=root)
        if error:
            status_code = 404 if "not found" in error.lower() else 500
            return jsonify(ok=False, error=error), status_code
        return jsonify(ok=True, files=files, root=root)
    except Exception as e:
        logger.exception("Failed to list snapshot files")
        return jsonify(ok=False, error=str(e)), 500


@route("/api/snapshot/delete", methods=["POST"])
async def snapshot_delete():
    data = await request.get_json()
    snapshot_id = data.get("snapshot", "")
    if not snapshot_id or not SNAPSHOT_ID_RE.match(snapshot_id):
        return jsonify(ok=False, error="Invalid snapshot id"), 400
    if op_lock.busy:
        return jsonify(ok=False, error=f"{op_lock.active.value} in progress"), 409
    try:
        ok = await delete_snapshot(snapshot_id)
        return jsonify(ok=ok)
    except Exception as e:
        logger.exception("Failed to delete snapshot")
        return jsonify(ok=False, error=str(e)), 500


@route("/api/check", methods=["POST"])
async def trigger_check():
    # `restic check` is read-only from the data's perspective but it does
    # acquire a repo lock, so don't run it on top of a backup/restore, and
    # don't spawn a second check if one is already in flight.
    if op_lock.busy:
        return jsonify(ok=False, error=f"{op_lock.active.value} in progress"), 409
    if check_running:
        return jsonify(ok=False, error="check already running"), 409
    asyncio.create_task(run_check())
    return jsonify(ok=True, message="Check started")


@route("/api/check/status")
async def check_status_endpoint():
    return jsonify(
        running=check_running,
        last_status=check_last_status,
        last_output=check_last_output,
        last_at=check_last_at,
    )


@route("/api/history")
async def backup_history():
    limit = min(int(request.args.get("limit", 20)), 100)
    offset = int(request.args.get("offset", 0))
    history, total = get_backup_history(limit, offset)
    return jsonify(ok=True, history=history, total=total)


@route("/api/backup/rename", methods=["POST"])
async def rename_backup():
    data = await request.get_json()
    backup_id = data.get("id")
    new_name = (data.get("name") or "").strip() or None
    if not backup_id:
        return jsonify(ok=False, error="Missing backup id"), 400
    conn = get_db()
    try:
        conn.execute("UPDATE backups SET name = ? WHERE id = ?", (new_name, backup_id))
        conn.commit()
        if conn.total_changes == 0:
            return jsonify(ok=False, error="Backup not found"), 404
    finally:
        conn.close()
    return jsonify(ok=True)


# ---------------------------------------------------------------------------
# App management & chown routes (pre-migration helpers)
# ---------------------------------------------------------------------------


async def _get_router_apps(router_token: str) -> dict:
    """Fetch app list from the local router.  Raises on failure."""
    import httpx

    async with httpx.AsyncClient(verify=False, timeout=10) as client:
        r = await client.get(
            f"{ROUTER_URL}/api/apps",
            headers={"Authorization": f"Bearer {router_token}"},
        )
        if r.status_code != 200:
            raise RuntimeError(f"Router returned {r.status_code}")
        return r.json()


@route("/api/apps-status")
async def apps_status():
    """Return the status of all apps from the local router."""
    router_token = _extract_bearer_token() or get_router_api_token()
    if not router_token:
        return jsonify(ok=False, error="No router API token configured"), 400
    try:
        apps = await _get_router_apps(router_token)
        return jsonify(ok=True, apps=apps)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500


@route("/api/stop-all-apps", methods=["POST"])
async def stop_all_apps():
    """Stop all running apps except the backup app."""
    router_token = _extract_bearer_token() or get_router_api_token()
    if not router_token:
        return jsonify(ok=False, error="No router API token configured"), 400
    try:
        import httpx

        apps = await _get_router_apps(router_token)
        stopped = []
        async with httpx.AsyncClient(verify=False, timeout=30) as client:
            for app_name, info in apps.items():
                if app_name == "backup":
                    continue
                if info.get("status") in ("running", "building", "starting"):
                    try:
                        sr = await client.post(
                            f"{ROUTER_URL}/stop_app/{app_name}",
                            headers={"Authorization": f"Bearer {router_token}"},
                        )
                        if sr.status_code == 200:
                            stopped.append(app_name)
                        else:
                            logger.warning(
                                "Failed to stop %s: HTTP %s", app_name, sr.status_code
                            )
                    except Exception as e:
                        logger.warning("Failed to stop %s: %s", app_name, e)
        return jsonify(ok=True, stopped=stopped)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500


# UIDs at or above this value are taken to be subuid-mapped — i.e. a host-side
# representation of a non-root user inside a rootless container's user
# namespace.  Distros conventionally allocate subuid ranges starting at
# 100000 (Debian/Ubuntu) or 165536 (the kernel-recommended floor); real
# interactive users are well below this.  Anything in this range is owned by
# a process running inside a container under a non-root in-container user
# (postgres, rabbitmq, mysql, etc.) and chowning it to the host user destroys
# the user-namespace mapping, leaving the in-container user unable to read
# its own data.
_SUBUID_FLOOR: int = 100000


@route("/api/chown-app-data", methods=["POST"])
async def chown_app_data():
    """Recursively chown app_data to the host user, skipping subuid-mapped files.

    Only allowed when all non-backup apps are stopped, to prevent
    ownership changes on files being actively written.

    Files whose current owner uid is at or above ``_SUBUID_FLOOR`` are
    assumed to be subuid-mapped state owned by a non-root in-container
    user (e.g. postgres at uid 70 → host uid 165605) and are left alone.
    Chowning those would destroy the user-namespace mapping and break the
    affected app.
    """
    router_token = _extract_bearer_token() or get_router_api_token()
    if not router_token:
        return jsonify(ok=False, error="No router API token configured"), 400

    # Check that all non-backup apps are stopped
    try:
        apps = await _get_router_apps(router_token)
        running = [
            name
            for name, info in apps.items()
            if name != "backup"
            and info.get("status") in ("running", "building", "starting")
        ]
        if running:
            return jsonify(
                ok=False,
                error=f"Apps still running: {', '.join(running)}. "
                "Stop all apps before fixing ownership.",
            ), 400
    except Exception as e:
        return jsonify(ok=False, error=f"Could not check app status: {e}"), 500

    # Run chown on the entire app_data directory.
    # We hardcode uid/gid 1000 (the default host user) because inside the
    # Docker container the parent directory is owned by root, so the
    # auto-detect logic in _fix_permissions would chown to root.
    if not ALL_APP_DATA.is_dir():
        return jsonify(
            ok=False, error=f"app_data directory not found: {ALL_APP_DATA}"
        ), 404

    target_uid = 1000
    target_gid = 1000
    app_data = str(ALL_APP_DATA)
    logger.info("chown -R %s:%s %s (skipping subuid-mapped files)", target_uid, target_gid, app_data)

    count = 0
    skipped = 0
    errors = 0

    def _chown_one(path: str) -> None:
        nonlocal count, skipped, errors
        try:
            st = os.lstat(path)
        except OSError as e:
            errors += 1
            logger.warning("stat failed for %s: %s", path, e)
            return
        if st.st_uid >= _SUBUID_FLOOR or st.st_gid >= _SUBUID_FLOOR:
            # Subuid-mapped state owned by a non-root in-container user.
            # Leaving it alone keeps that app working.
            skipped += 1
            return
        try:
            os.chown(path, target_uid, target_gid, follow_symlinks=False)
            count += 1
        except OSError as e:
            errors += 1
            logger.warning("chown failed for %s: %s", path, e)

    for root, dirs, files in os.walk(app_data):
        for name in dirs + files:
            _chown_one(os.path.join(root, name))
    _chown_one(app_data)

    logger.info("chown complete: %d items fixed, %d skipped, %d errors", count, skipped, errors)
    return jsonify(
        ok=True,
        message=f"Ownership fixed on {count} items (uid={target_uid}, gid={target_gid}); "
        f"skipped {skipped} subuid-mapped items",
        count=count,
        skipped=skipped,
        errors=errors,
    )


# ---------------------------------------------------------------------------
# Migration routes
# ---------------------------------------------------------------------------


@route("/api/migration/status")
async def migration_status_endpoint():
    return jsonify(
        running=op_lock.migration_running,
        status=migration.status,
        log=migration.log[-50:],
    )


# ---------------------------------------------------------------------------
# Direct push migration
# ---------------------------------------------------------------------------


@route("/api/migration/push", methods=["POST"])
async def trigger_direct_push():
    """One-click migration: push all apps + data to another instance."""
    err = op_lock.try_acquire(OpKind.MIGRATION)
    if err:
        return jsonify(ok=False, error=err), 409

    data = await request.get_json(silent=True) or {}
    target_url = (data.get("target_url") or "").rstrip("/")
    target_token = data.get("target_token") or ""

    if not target_url:
        op_lock.release(OpKind.MIGRATION)
        return jsonify(ok=False, error="Missing target_url"), 400
    if not target_token:
        op_lock.release(OpKind.MIGRATION)
        return jsonify(ok=False, error="Missing target_token"), 400

    selected_apps = data.get("apps")  # optional list
    if selected_apps is not None and not isinstance(selected_apps, list):
        op_lock.release(OpKind.MIGRATION)
        return jsonify(ok=False, error="'apps' must be a list"), 400

    router_token = _extract_bearer_token() or get_router_api_token()

    # Pre-flight check: verify the router token works before starting.
    # Without a valid token the migration will fail when it tries to
    # list apps on this instance.
    if router_token:
        import httpx

        try:
            async with httpx.AsyncClient(verify=False, timeout=10) as client:
                r = await client.get(
                    f"{ROUTER_URL}/api/apps",
                    headers={"Authorization": f"Bearer {router_token}"},
                )
                if (
                    r.status_code != 200
                    or r.headers.get("content-type", "").find("json") == -1
                ):
                    op_lock.release(OpKind.MIGRATION)
                    return jsonify(
                        ok=False,
                        error="Router API token is invalid or expired. "
                        "Go to the Backups tab and set a valid router_api_token "
                        "in the backup config (POST /api/config with "
                        '{"router_api_token": "..."}). '
                        "You can generate a token from the OpenHost dashboard "
                        "under API Tokens.",
                    ), 400
        except Exception:
            pass  # Network issue; let the migration try anyway
    else:
        op_lock.release(OpKind.MIGRATION)
        return jsonify(
            ok=False,
            error="No router API token configured. The backup app needs a "
            "token to access the local router API during migration. "
            "Set one via the backup config: POST /api/config with "
            '{"router_api_token": "YOUR_TOKEN"}. You can generate '
            "a token from the OpenHost dashboard under API Tokens.",
        ), 400

    asyncio.create_task(
        migration.run_direct_push(
            target_url=target_url,
            target_token=target_token,
            selected_apps=selected_apps,
            lock=op_lock,
            all_app_data=ALL_APP_DATA,
            vm_data_dir=VM_DATA_DIR,
            router_url=ROUTER_URL,
            zone_domain=ZONE_DOMAIN,
            router_token=router_token,
        )
    )
    return jsonify(ok=True, message="Direct push migration started")


# ---------------------------------------------------------------------------
# Receive endpoints (target side — called by source during direct push)
# ---------------------------------------------------------------------------


@route("/api/migration/receive/start", methods=["POST"])
async def receive_start():
    """Accept a migration manifest from a source instance.

    Acquires the operation lock to prevent concurrent backup/restore,
    stops all non-backup apps, and deletes app data for migrated apps.
    The lock is held until receive_finalize completes.
    """
    err = op_lock.try_acquire(OpKind.MIGRATION)
    if err:
        return jsonify(ok=False, error=err), 409
    data = await request.get_json(silent=True) or {}
    if not data:
        op_lock.release(OpKind.MIGRATION)
        return jsonify(ok=False, error="Missing manifest"), 400
    router_token = _extract_bearer_token() or get_router_api_token()
    try:
        result = await migration.receive_start(
            data, ALL_APP_DATA, ROUTER_URL, router_token
        )
    except Exception as e:
        logger.exception("receive_start failed")
        op_lock.release(OpKind.MIGRATION)
        return jsonify(ok=False, error=str(e)), 500
    if not result.get("ok"):
        op_lock.release(OpKind.MIGRATION)
    # Lock stays held on success — released by receive_finalize
    code = 200 if result.get("ok") else 400
    return jsonify(**result), code


@route("/api/migration/receive/app/<app_name>", methods=["POST"])
async def receive_app(app_name):
    """Receive a tar.gz stream of a single app's data (backward compat)."""
    if not migration.validate_name(app_name):
        return jsonify(ok=False, error="Invalid app name"), 400
    tar_data = await request.get_data()
    if not tar_data:
        return jsonify(ok=False, error="Empty request body"), 400
    result = await migration.receive_app_data(app_name, tar_data, ALL_APP_DATA)
    code = 200 if result.get("ok") else 400
    return jsonify(**result), code


@route("/api/migration/receive/chunk/<app_name>", methods=["POST"])
async def receive_chunk(app_name):
    """Receive one chunk of a large app's tar.gz data."""
    if not migration.validate_name(app_name):
        return jsonify(ok=False, error="Invalid app name"), 400
    chunk_data = await request.get_data()
    if not chunk_data:
        return jsonify(ok=False, error="Empty chunk"), 400
    chunk_index = int(request.headers.get("X-Chunk-Index", "0"))
    is_final = request.headers.get("X-Chunk-Final", "0") == "1"
    result = await migration.receive_chunk(
        app_name, chunk_data, chunk_index, is_final, ALL_APP_DATA
    )
    code = 200 if result.get("ok") else 400
    return jsonify(**result), code


@route("/api/migration/receive/data", methods=["POST"])
async def receive_data():
    """Receive a tar.gz stream of all app data.

    Streams the request body to a temp file to avoid buffering the
    entire archive in memory, then extracts from the file.
    """
    import tempfile as _tempfile

    # Stream request body to a temp file instead of buffering in memory.
    # Quart's request.get_data() would load everything into RAM; for
    # multi-GB archives that OOMs the container.
    tmp = _tempfile.NamedTemporaryFile(
        dir=str(APP_DATA_DIR), suffix=".tar.gz", delete=False
    )
    try:
        total = 0
        async for chunk in request.body:
            tmp.write(chunk)
            total += len(chunk)
        tmp.close()

        if total == 0:
            os.unlink(tmp.name)
            return jsonify(ok=False, error="Empty request body"), 400

        result = await migration.receive_all_data_from_file(tmp.name, ALL_APP_DATA)
        code = 200 if result.get("ok") else 400
        return jsonify(**result), code
    except Exception as e:
        logger.exception("receive_data failed")
        return jsonify(ok=False, error=str(e)), 500
    finally:
        try:
            os.unlink(tmp.name)
        except OSError:
            pass


@route("/api/migration/receive/finalize", methods=["POST"])
async def receive_finalize():
    """Deploy/restart apps after data has been received. Releases the op lock."""
    data = await request.get_json(silent=True) or {}
    manifest = data.get("manifest", {})
    if not manifest:
        if op_lock.active == OpKind.MIGRATION:
            op_lock.release(OpKind.MIGRATION)
        return jsonify(ok=False, error="Missing manifest"), 400
    repo_urls = data.get("repo_urls")
    router_token = _extract_bearer_token() or get_router_api_token()
    try:
        result = await migration.receive_finalize(
            manifest, ROUTER_URL, router_token, repo_urls=repo_urls
        )
        return jsonify(**result)
    finally:
        # Only release if we actually hold the migration lock
        if op_lock.active == OpKind.MIGRATION:
            op_lock.release(OpKind.MIGRATION)


@route("/health")
async def health():
    return "ok"
