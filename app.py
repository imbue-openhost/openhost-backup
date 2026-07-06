import asyncio
import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from quart import Quart, jsonify, render_template, request

import migration
from operations import OperationLock, OpKind

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
# Hostname recorded on every snapshot (`restic backup --host`). The container's
# own hostname is random and changes on each restart/redeploy, which would
# fragment `restic forget --group-by host` into per-container groups. Pinning it
# to the (stable) zone domain gives every snapshot from this instance one
# identity — and, in a shared repo, keeps instances distinguishable. Falls back
# to a constant when the zone domain isn't set so we never pass an empty --host.
BACKUP_HOST = ZONE_DOMAIN or "openhost"
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
    "interval_seconds": 0,
    "repo": "",
    "repo_password": "",
    "env": {},
    # Retention policy (restic `forget` keep-* flags). 0 = tier unset. When
    # every tier is 0 no retention runs, so the default is "keep everything".
    "keep_last": 0,
    "keep_hourly": 0,
    "keep_daily": 0,
    "keep_weekly": 0,
    "keep_monthly": 0,
    "keep_yearly": 0,
}

# Config key -> restic forget flag. Order is cosmetic (matches restic docs).
KEEP_FLAGS = {
    "keep_last": "--keep-last",
    "keep_hourly": "--keep-hourly",
    "keep_daily": "--keep-daily",
    "keep_weekly": "--keep-weekly",
    "keep_monthly": "--keep-monthly",
    "keep_yearly": "--keep-yearly",
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

# A migration holds op_lock across many separate receive_* requests (start ->
# chunks -> finalize). If the source dies mid-transfer the destination never
# gets a finalize, so the lock would otherwise stay held until an app restart
# (issue #14). Treat a migration idle this long as abandoned and reclaim it.
MIGRATION_IDLE_TIMEOUT_SECONDS = 30 * 60


def _reclaim_abandoned_migration() -> None:
    """Release a migration lock orphaned by a dead/stopped source (issue #14).

    Safe to call before starting any operation: it only clears a *migration*
    lock idle past the timeout, so a live transfer (kept fresh via
    ``op_lock.touch()`` on each receive) is never disturbed.
    """
    op_lock.release_if_stale(OpKind.MIGRATION, MIGRATION_IDLE_TIMEOUT_SECONDS)


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
        ("repo_size_bytes", "ALTER TABLE backups ADD COLUMN repo_size_bytes INTEGER"),
        (
            "repo_uncompressed_bytes",
            "ALTER TABLE backups ADD COLUMN repo_uncompressed_bytes INTEGER",
        ),
        ("repo_blob_count", "ALTER TABLE backups ADD COLUMN repo_blob_count INTEGER"),
        (
            "repo_snapshots_count",
            "ALTER TABLE backups ADD COLUMN repo_snapshots_count INTEGER",
        ),
        (
            "repo_compression_ratio",
            "ALTER TABLE backups ADD COLUMN repo_compression_ratio REAL",
        ),
        ("repo_stats_at", "ALTER TABLE backups ADD COLUMN repo_stats_at TEXT"),
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
    repo_stats=None,
):
    """Insert a backup record into the database.

    When ``repo_stats`` (a dict from ``repo_stats()``) is supplied, the
    repo-wide size cache is written into the *same* INSERT, so a successful
    backup records both its per-run figures and the current repo footprint
    in one row — no separate connection or ``MAX(id)`` update needed. The
    delete path, which has no INSERT to piggyback on, instead re-stamps the
    newest surviving row inline (see ``delete_snapshot``).
    """
    cols = [
        "timestamp",
        "status",
        "error_message",
        "snapshot_id",
        "data_added_bytes",
        "total_size_bytes",
        "file_count",
        "name",
    ]
    vals = [
        timestamp,
        status,
        error_message,
        snapshot_id,
        data_added_bytes,
        total_size_bytes,
        file_count,
        name,
    ]
    if repo_stats is not None:
        cols += [
            "repo_size_bytes",
            "repo_uncompressed_bytes",
            "repo_blob_count",
            "repo_snapshots_count",
            "repo_compression_ratio",
            "repo_stats_at",
        ]
        vals += [
            repo_stats.get("total_size_bytes"),
            repo_stats.get("total_uncompressed_size_bytes"),
            repo_stats.get("total_blob_count"),
            repo_stats.get("snapshots_count"),
            repo_stats.get("compression_ratio"),
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        ]
    placeholders = ", ".join(["?"] * len(vals))
    conn = get_db()
    try:
        conn.execute(
            f"INSERT INTO backups ({', '.join(cols)}) VALUES ({placeholders})",
            vals,
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


def load_repo_stats_cache() -> dict | None:
    """Return the last cached repo stats (newest stamped row), or None.

    The cache is written by whoever changes the repo footprint, folded into
    that operation's own DB write — ``record_backup`` for a backup,
    ``delete_snapshot`` for a prune — so there is no standalone writer here.
    Keys mirror what ``repo_stats()`` returns so /api/repo/stats can serve
    this verbatim, plus ``computed_at`` so the UI can show its age.
    """
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT repo_size_bytes, repo_uncompressed_bytes, repo_blob_count, "
            "repo_snapshots_count, repo_compression_ratio, repo_stats_at "
            "FROM backups WHERE repo_stats_at IS NOT NULL ORDER BY id DESC LIMIT 1"
        ).fetchone()
    except sqlite3.Error:
        logger.exception("Failed to read cached repo stats")
        return None
    finally:
        conn.close()
    if not row:
        return None
    return {
        "total_size_bytes": row[0],
        "total_uncompressed_size_bytes": row[1],
        "total_blob_count": row[2],
        "snapshots_count": row[3],
        "compression_ratio": row[4],
        "computed_at": row[5],
    }


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
            return r.status_code == 200 and "json" in r.headers.get("content-type", "")
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

    Every invocation is logged at INFO (command on start, exit code +
    elapsed time on completion) so the app's console — ``oh app logs
    backup`` — shows exactly what restic ran. The args never carry secrets:
    the repo URL and password go through the environment (see
    ``_restic_env``), not argv.
    """
    env = _restic_env(conf)
    logger.info("restic %s", " ".join(args))
    started = time.monotonic()
    proc = await asyncio.create_subprocess_exec(
        "restic",
        *args,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except (asyncio.TimeoutError, asyncio.CancelledError) as e:
        logger.warning(
            "restic %s killed after %.1fs (%s)",
            args[0] if args else "?",
            time.monotonic() - started,
            type(e).__name__,
        )
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
    logger.info(
        "restic %s -> rc=%s (%.1fs)",
        args[0] if args else "?",
        proc.returncode,
        time.monotonic() - started,
    )
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
FORGET_TIMEOUT_SECONDS = 10 * 60  # 10 minutes — forget only rewrites metadata
PRUNE_TIMEOUT_SECONDS = 6 * 60 * 60  # 6 hours — prune repacks, can be slow on S3

# Guard against concurrent `restic init` calls. When the UI loads, multiple
# API endpoints (snapshots, stats, check) call ensure_repo_initialized at
# the same time. Without this lock, two concurrent `restic init` invocations
# can corrupt the repo (the second init races with the first, producing keys
# that fail ciphertext verification).
_init_lock = asyncio.Lock()


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
    async with _init_lock:
        # `cat config` is a cheap way to confirm the repo exists and the password
        # is correct. It returns non-zero on either missing repo or wrong password.
        rc, _stdout, stderr = await _run_restic(
            ["cat", "config", "--no-lock"], conf, timeout=30
        )
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
                return (
                    False,
                    f"restic init failed: {err2.decode(errors='replace').strip()}",
                )
            return True, None
        return False, f"restic repo check failed: {err}"


async def _restic_unlock_if_stale(conf: dict) -> None:
    """Best-effort remove stale repo locks at startup.

    Uses ``--remove-all`` so that locks left by a previous container
    incarnation are cleared even if the hostname changed between restarts
    (which is the normal case for Docker containers — each restart gets a
    new random hostname, so plain ``restic unlock`` would only remove locks
    matching the current hostname and silently leave the stale one behind).
    """
    try:
        rc, stdout, stderr = await _run_restic(
            ["unlock", "--remove-all"], conf, timeout=30
        )
        if rc == 0:
            out = (
                stdout.decode(errors="replace") + stderr.decode(errors="replace")
            ).strip()
            if out:
                logger.info("restic unlock --remove-all: %s", out)
            else:
                logger.info("restic unlock --remove-all: no stale locks found")
        else:
            err = (
                stdout.decode(errors="replace") + stderr.decode(errors="replace")
            ).strip()
            logger.warning("restic unlock --remove-all failed (rc=%d): %s", rc, err)
    except Exception:
        logger.warning("restic unlock failed on startup", exc_info=True)


async def test_restic_connection(
    conf: dict, *, timeout: float = 10.0
) -> tuple[bool, str, str]:
    """Run ``restic cat config`` once with a short timeout, no retries.

    Returns ``(ok, output)`` where ``output`` is the raw stderr restic
    produced. We drain stderr incrementally in a background task so that
    when we kill restic at the timeout, all the bytes it printed up to
    that point — typically several ``retrying after Xs: <backend error>``
    lines — are already in our buffer.
    """
    env = _restic_env(conf)
    logger.info("restic cat config (connection test, timeout=%.0fs)", timeout)
    proc = await asyncio.create_subprocess_exec(
        "restic",
        "cat",
        "config",
        "--no-lock",
        env=env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    buf = bytearray()

    async def drain() -> None:
        while True:
            chunk = await proc.stderr.read(4096)
            if not chunk:
                return
            buf.extend(chunk)

    drain_task = asyncio.create_task(drain())
    timed_out = False
    try:
        await asyncio.wait_for(proc.wait(), timeout=timeout)
    except asyncio.TimeoutError:
        timed_out = True
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        try:
            await proc.wait()
        except Exception:
            pass
    try:
        await asyncio.wait_for(drain_task, timeout=2)
    except asyncio.TimeoutError:
        drain_task.cancel()

    text = bytes(buf).decode(errors="replace")
    if timed_out:
        text = (text + f"\n\n[killed after {timeout:.0f}s — no retries]").lstrip()
    return _classify_restic_test(proc.returncode, text, timed_out)


def _classify_restic_test(
    returncode: int | None, output: str, timed_out: bool
) -> tuple[bool, str, str]:
    """Decide whether a restic test should read as success or failure.

    Exit 0 → success.
    "repository does not exist" → success ("reachable, just no repo yet" —
    a backup will create it). The bucket / path / creds all worked; the
    only "missing" thing is the user hasn't initialized a repo there yet,
    which is the expected state on first run.
    Anything else → failure.
    """
    if returncode == 0 and not timed_out:
        return True, "Connection OK", output or "(no output)"
    if not timed_out and "repository does not exist" in output.lower():
        return (
            True,
            "Reachable — no repository at this location yet (a backup will create it)",
            output,
        )
    return False, "Connection failed", output or f"restic exited with code {returncode}"


def _build_restic_debug(conf: dict) -> dict:
    """Return the restic command + env used for testing.

    All values are included in plaintext — this app has no public routes,
    so callers are already authed as the owner by the OpenHost router.
    The UI hides the secret-looking values behind a 'Show secrets' button
    purely as a shoulder-surfing guard.
    """
    env = _restic_env(conf)
    # Only the keys restic actually reads from us, not the whole process env.
    keys: list[str] = ["RESTIC_REPOSITORY", "RESTIC_PASSWORD"]
    for k in conf.get("env") or {}:
        if k not in keys:
            keys.append(k)
    entries = []
    for k in keys:
        v = env.get(k, "")
        entries.append({"key": k, "value": v})
    return {
        "command": "restic cat config",
        "env": entries,
    }


# ---------------------------------------------------------------------------
# Backup
# ---------------------------------------------------------------------------


async def run_backup(name: str | None = None) -> bool:
    _reclaim_abandoned_migration()
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

    removed = 0
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
            msg = "No backup roots available — expected one of: " + ", ".join(
                str(p) for p in BACKUP_ROOTS
            )
            record_backup(timestamp, "error", msg, name=name)
            logger.error(msg)
            return False

        args = ["backup", "--json"]
        args += [str(p) for p in roots]
        # Pin the recorded hostname so every snapshot from this instance shares
        # one stable identity (see BACKUP_HOST) instead of the container's
        # random per-restart hostname.
        args += ["--host", BACKUP_HOST]
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
            rc, stdout, stderr = await _run_restic(
                args, conf, timeout=BACKUP_TIMEOUT_SECONDS
            )
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

        if rc == 0:
            # Backup succeeded. Apply the retention policy first (forget only,
            # under the lock) so the footprint we stamp reflects the
            # post-retention snapshot set. Best-effort — a retention failure
            # must not fail the backup itself.
            try:
                removed = await run_retention(conf)
            except Exception:
                logger.exception("Retention failed")

            # Compute the repo footprint now, while we still hold the op lock
            # (so the stats read stays serialized), and fold it into the same
            # row record_backup inserts — no second connection or MAX(id)
            # update. repo_stats() is best-effort and never raises; on failure
            # repo_stats_data is None and the row just carries no fresh cache
            # (the reader falls back to the previous stamped row). The size is
            # still pre-prune here; the background prune re-stamps it once it
            # reclaims space.
            repo_stats_data, _ = await repo_stats()
            record_backup(
                timestamp,
                "success",
                snapshot_id=summary.get("snapshot_id") if summary else None,
                data_added_bytes=summary.get("data_added") if summary else None,
                total_size_bytes=(
                    summary.get("total_bytes_processed") if summary else None
                ),
                file_count=summary.get("total_files_processed") if summary else None,
                name=name,
                repo_stats=repo_stats_data,
            )
            if summary is not None:
                logger.info(
                    "Backup completed: snapshot=%s data_added=%s total=%s",
                    summary.get("snapshot_id", "?"),
                    summary.get("data_added", "?"),
                    summary.get("total_bytes_processed", "?"),
                )
            else:
                # Succeeded but we somehow missed the summary line.
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
        # Retention forgot snapshots but didn't prune — reclaim the space in
        # the background so it doesn't extend the backup or hold the lock for
        # prune's full duration. Scheduled after the lock is released so the
        # worker can acquire it.
        if removed:
            schedule_prune()


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
            ["snapshots", "--no-lock", "--json", "--tag", "openhost"], conf, timeout=60
        )
        if rc != 0:
            logger.error(
                "restic snapshots failed: %s", stderr.decode(errors="replace").strip()
            )
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
        # Reconcile the history DB against reality: this listing just
        # succeeded, so any backups row whose snapshot isn't here (retention
        # forgot it, or it was deleted out of band) is stale. Reuses this
        # call's result — no extra restic invocation.
        _reconcile_snapshots_db({e["id"] for e in out if e.get("id")})
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
            ["stats", "--no-lock", "--mode", "raw-data", "--json", "--tag", "openhost"],
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
        args = ["ls", "--no-lock", "--json", snapshot_id, str(path)]
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

    args = ["ls", "--no-lock", "--json", snapshot_id, target_path]
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

    # Prune reclaimed space, so the cached repo size is now stale. Recompute
    # it here (best-effort, still off the request path — this runs in the
    # delete task, not a /api/repo/stats request) and re-stamp it below.
    repo_stats_data = (await repo_stats())[0]

    # DB cleanup. Snapshot IDs stored here are always the full 64-char IDs
    # that restic emits in its --json summary, so an exact match on the
    # user-supplied ID is sufficient when they pass a full ID. When they
    # pass a short (8-char) ID, match by prefix with length >= 8 to avoid
    # accidental matches on arbitrary substrings.
    conn = get_db()
    try:
        if len(snapshot_id) >= 40:
            conn.execute("DELETE FROM backups WHERE snapshot_id = ?", (snapshot_id,))
        else:
            conn.execute(
                "DELETE FROM backups WHERE substr(snapshot_id, 1, ?) = ?",
                (len(snapshot_id), snapshot_id),
            )
        # Re-stamp the fresh size onto the newest *surviving* backup row, in
        # the same connection as the delete. Unlike a backup (which folds its
        # stats into its own INSERT), a delete has no row of its own, so
        # MAX(id) is unavoidable here. No-op if no rows remain or stats failed.
        if repo_stats_data is not None:
            conn.execute(
                "UPDATE backups SET repo_size_bytes = ?, repo_uncompressed_bytes = ?, "
                "repo_blob_count = ?, repo_snapshots_count = ?, "
                "repo_compression_ratio = ?, repo_stats_at = ? "
                "WHERE id = (SELECT MAX(id) FROM backups)",
                (
                    repo_stats_data.get("total_size_bytes"),
                    repo_stats_data.get("total_uncompressed_size_bytes"),
                    repo_stats_data.get("total_blob_count"),
                    repo_stats_data.get("snapshots_count"),
                    repo_stats_data.get("compression_ratio"),
                    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                ),
            )
        conn.commit()
    except sqlite3.Error:
        # The restic forget already succeeded; don't fail the operation.
        logger.exception("DB cleanup failed for snapshot %s", snapshot_id)
    finally:
        conn.close()
    logger.info("Deleted snapshot %s", snapshot_id)
    return True


# ---------------------------------------------------------------------------
# Retention (restic forget) + background prune
# ---------------------------------------------------------------------------


def _forget_args(conf: dict) -> list[str] | None:
    """Build ``restic forget`` args from the configured keep-* policy.

    Returns None when no tier is set — meaning "keep everything", so no
    forget runs. This is also the safety floor: we never issue a forget with
    zero keep flags, which would delete every snapshot. ``--prune`` is
    intentionally omitted — it runs in the background afterwards (see
    ``schedule_prune``).

    Scoping: ``--tag openhost`` selects our snapshots and ``--group-by ''``
    (empty) treats them all as ONE group so the policy applies across the
    whole set. We assume a single instance per repo, so no per-host/paths
    grouping is needed — and grouping would only fragment retention (paths
    vary when a root like vm_data is absent; host varied on older snapshots
    before we began pinning ``--host BACKUP_HOST``). Backups are pinned to
    the zone host from now on for a stable snapshot identity.

    Values are stored as validated ints by ``post_config`` and seeded by
    ``DEFAULT_CONFIG``, so we can read them directly.
    """
    keeps: list[str] = []
    for key, flag in KEEP_FLAGS.items():
        if conf.get(key):
            keeps += [flag, str(conf[key])]
    if not keeps:
        return None
    return ["forget", "--json", "--tag", "openhost", "--group-by", "", *keeps]


def _reconcile_snapshots_db(present_ids: set[str]) -> None:
    """Drop history rows whose snapshot no longer exists in the repo.

    The backstop that keeps the ``backups`` table a subset of restic reality
    — covers retention's forgotten snapshots plus anything deleted out of
    band. Rows with no ``snapshot_id`` (successful backups whose summary was
    missing) are left alone. Only call with the ids from a *successful*
    listing: an empty set from a failed list would wipe every row.
    """
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT id, snapshot_id FROM backups WHERE snapshot_id IS NOT NULL"
        ).fetchall()
        stale = [(r[0],) for r in rows if r[1] not in present_ids]
        if stale:
            conn.executemany("DELETE FROM backups WHERE id = ?", stale)
            conn.commit()
            logger.info("Reconciled DB: removed %d stale backup row(s)", len(stale))
    except sqlite3.Error:
        logger.exception("Snapshot DB reconcile failed")
    finally:
        conn.close()


async def run_retention(conf: dict) -> int:
    """Apply the keep-* policy via ``restic forget`` and reconcile the DB.

    Returns the number of snapshots forgotten. Prune (the expensive step
    that reclaims space) is deliberately NOT run here — the caller schedules
    it in the background only when this returns > 0. Must be called while
    holding the operation lock.
    """
    args = _forget_args(conf)
    if args is None:
        return 0
    try:
        rc, stdout, stderr = await _run_restic(
            args, conf, timeout=FORGET_TIMEOUT_SECONDS
        )
    except asyncio.TimeoutError:
        logger.error("restic forget timed out after %ss", FORGET_TIMEOUT_SECONDS)
        return 0
    if rc != 0:
        logger.error(
            "restic forget failed: %s", stderr.decode(errors="replace").strip()
        )
        return 0

    # forget --json returns one object per group, each with a "remove" list of
    # snapshot objects (absent/empty when nothing was removed).
    removed_ids: list[str] = []
    try:
        for group in json.loads(stdout.decode(errors="replace") or "[]"):
            for snap in group.get("remove") or []:
                sid = snap.get("id")
                if sid:
                    removed_ids.append(sid)
    except (json.JSONDecodeError, AttributeError):
        logger.warning("Could not parse restic forget --json output")

    if removed_ids:
        conn = get_db()
        try:
            conn.executemany(
                "DELETE FROM backups WHERE snapshot_id = ?",
                [(sid,) for sid in removed_ids],
            )
            conn.commit()
        except sqlite3.Error:
            logger.exception("DB cleanup after forget failed")
        finally:
            conn.close()
        logger.info("Retention forgot %d snapshot(s)", len(removed_ids))
    return len(removed_ids)


# Background prune coordination. restic prune reclaims the space freed by
# forget; it's slow (repacks pack files) so we run it off the backup path as a
# single coalesced worker. _prune_needed lets a forget that lands while a prune
# is already running request a follow-up pass.
_prune_needed = False
_prune_task: "asyncio.Task | None" = None


def schedule_prune() -> None:
    """Request a background prune. Coalesces: at most one worker runs at once."""
    global _prune_needed, _prune_task
    _prune_needed = True
    if _prune_task is None or _prune_task.done():
        _prune_task = asyncio.create_task(_prune_worker())


async def _prune_worker() -> None:
    global _prune_needed
    while _prune_needed:
        _prune_needed = False
        # A prune must not run concurrently with a backup/restore/migration —
        # restic takes an exclusive repo lock — so wait for the operation lock.
        waited = 0.0
        while op_lock.try_acquire(OpKind.PRUNE) is not None:
            if waited >= PRUNE_TIMEOUT_SECONDS:
                logger.warning("Prune gave up waiting for the operation lock")
                _prune_needed = True  # retry on the next schedule_prune
                return
            await asyncio.sleep(5)
            waited += 5
        try:
            await _run_prune_locked()
        finally:
            op_lock.release(OpKind.PRUNE)


async def _run_prune_locked() -> None:
    """Run ``restic prune`` and re-stamp the repo-size cache. Lock held by caller."""
    conf = load_config()
    if not conf.get("repo") or not conf.get("repo_password"):
        return
    try:
        rc, _out, stderr = await _run_restic(
            ["prune"], conf, timeout=PRUNE_TIMEOUT_SECONDS
        )
    except asyncio.TimeoutError:
        logger.error("restic prune timed out after %ss", PRUNE_TIMEOUT_SECONDS)
        return
    if rc != 0:
        logger.error("restic prune failed: %s", stderr.decode(errors="replace").strip())
        return
    logger.info("Prune completed")
    # Prune reclaimed space, so the cached repo size is stale — recompute and
    # re-stamp the newest surviving backup row.
    stats = (await repo_stats())[0]
    if stats is not None:
        conn = get_db()
        try:
            conn.execute(
                "UPDATE backups SET repo_size_bytes = ?, repo_uncompressed_bytes = ?, "
                "repo_blob_count = ?, repo_snapshots_count = ?, "
                "repo_compression_ratio = ?, repo_stats_at = ? "
                "WHERE id = (SELECT MAX(id) FROM backups)",
                (
                    stats.get("total_size_bytes"),
                    stats.get("total_uncompressed_size_bytes"),
                    stats.get("total_blob_count"),
                    stats.get("snapshots_count"),
                    stats.get("compression_ratio"),
                    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                ),
            )
            conn.commit()
        except sqlite3.Error:
            logger.exception("Failed to re-stamp repo stats after prune")
        finally:
            conn.close()


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

    _reclaim_abandoned_migration()
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

    logger.info("Starting restic restore from %s (root=%s)", snapshot_id, root or "all")

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
            restore_last_status = f"error: {stderr.decode(errors='replace').strip() or f'restic exit {rc}'}"
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
        output = (
            stdout.decode(errors="replace") + stderr.decode(errors="replace")
        ).strip()
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

        if not interval or not conf.get("repo"):
            await asyncio.sleep(30)
            continue

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
    """Make sure config.json exists with default values.

    Does not auto-generate a password or set a repo — the user configures
    those through the UI. Backups won't run until configured.
    """
    if not CONFIG_FILE.exists():
        save_config(load_config())


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
    env_pairs = conf.get("env") or {}
    # Render env as "KEY=val;KEY2=val2" — same shape the input accepts on save.
    env_string = ";".join(f"{k}={v}" for k, v in env_pairs.items())
    return await render_template(
        "index.html",
        base_path=BASE_PATH,
        config=conf,
        env_string=env_string,
        state=state,
        backend=backend,
        scope=_backup_scope_summary(),
    )


def _backup_scope_summary() -> dict:
    """Snapshot the scope of what backup currently captures and skips.

    Surfaced in the UI so the user can tell, at a glance, that
    ``/data/app_archive`` is intentionally outside the snapshot —
    important because access_all_data mounts the archive into the
    backup container and the file-browser path can otherwise leave
    the impression that those bytes will be in the next snapshot.

    Built off the same ``BACKUP_ROOTS`` / ``BACKUP_EXCLUDES`` tuples
    that the backup + restore code paths use, so the UI can never
    drift from the actual restic command line.  Each entry carries
    a short ``reason`` string suitable for inline rendering.

    ``present`` reflects whether the path exists on disk now; the
    backup loop skips missing roots (instances may grant only a
    subset of data permissions), so showing this lets the operator
    distinguish "permission not granted" from "present and excluded".
    """
    included = []
    for p in BACKUP_ROOTS:
        included.append({"path": str(p), "present": p.is_dir()})

    excluded = []
    for p in BACKUP_EXCLUDES:
        # ``user_facing=False`` marks an exclude that's an
        # implementation detail (the backup app's own restic repo
        # dir) rather than something the operator chose to keep
        # outside the snapshot pipeline.  Surfaced this way so the
        # snapshots-browser note can hide self-references without
        # the JS having to hard-code which path that is — the JS
        # filters on ``user_facing`` and stays in lockstep with
        # whatever the helper decides counts as operator-relevant.
        if p == APP_ARCHIVE:
            reason = (
                "Archive tier is its own durable store (S3 bucket or "
                "host-managed local archive); double-storing through "
                "restic would inflate snapshots without adding safety."
            )
            user_facing = True
        elif p == ALL_APP_DATA / "backup":
            reason = (
                "The backup app's own data dir contains the restic "
                "repository — including it would self-reference and "
                "grow each snapshot unboundedly."
            )
            user_facing = False
        else:
            reason = ""
            user_facing = True
        excluded.append(
            {
                "path": str(p),
                "present": p.exists(),
                "reason": reason,
                "user_facing": user_facing,
            }
        )

    return {"included": included, "excluded": excluded}


@route("/api/config", methods=["GET"])
async def get_config():
    conf = load_config()
    return jsonify(config={**conf, "backend": classify_repo(conf.get("repo", ""))})


@route("/api/config", methods=["POST"])
async def post_config():
    data = await request.get_json()
    current_conf = load_config()

    # router_api_token is special: it lets this app call the OpenHost
    # router. After it's been set once, require a Bearer token to rotate
    # it so a co-located container can't quietly swap it for one they
    # control.
    if "router_api_token" in data and current_conf.get("router_api_token"):
        if not await _verify_admin_token(_extract_bearer_token()):
            return jsonify(
                ok=False,
                error="Bearer token required to rotate router_api_token",
            ), 401

    conf = current_conf
    for key in ("repo", "repo_password", "router_api_token"):
        if key in data:
            conf[key] = data[key] or ""
    if "env" in data:
        if not isinstance(data["env"], dict):
            return jsonify(ok=False, error="'env' must be an object"), 400
        conf["env"] = {
            k: str(v) for k, v in data["env"].items() if v != "" and v is not None
        }
    if "interval_seconds" in data:
        try:
            interval = int(data["interval_seconds"])
        except (TypeError, ValueError):
            return jsonify(ok=False, error="interval_seconds must be an integer"), 400
        conf["interval_seconds"] = 0 if interval <= 0 else max(60, interval)
    # Retention policy (restic forget keep-* tiers). Each is a non-negative
    # int; 0 = tier unset. Stored as ints so _forget_args can read them
    # directly.
    for key in KEEP_FLAGS:
        if key in data:
            try:
                n = int(data[key])
            except (TypeError, ValueError):
                return jsonify(ok=False, error=f"{key} must be an integer"), 400
            conf[key] = max(0, n)
    save_config(conf)
    return jsonify(ok=True)


@route("/api/repo/test", methods=["POST"])
async def api_repo_test():
    """Test the restic connection once, no retries.

    Body (all optional): ``repo``, ``repo_password`` — override the saved
    values for the test, so the user can try a new URL/password before
    committing them with Save. The saved ``env`` is always used.
    """
    data = await request.get_json(silent=True) or {}
    conf = load_config()
    repo = (data.get("repo") or conf.get("repo") or "").strip()
    if not repo:
        return jsonify(ok=False, error="No repo URL configured"), 400
    repo_password = data.get("repo_password") or conf.get("repo_password", "")
    # Merge env overrides on top of saved env so the user can test credentials
    # they've typed into the page (AWS quick setup, raw env setter) without
    # having to Apply/Save first.
    env_override = data.get("env") or {}
    merged_env = {**(conf.get("env") or {})}
    for k, v in env_override.items():
        if v is None or v == "":
            continue
        merged_env[k] = v
    test_conf = {
        **conf,
        "repo": repo,
        "repo_password": repo_password,
        "env": merged_env,
    }

    ok, message, output = await test_restic_connection(test_conf)
    debug = _build_restic_debug(test_conf)
    return jsonify(ok=ok, message=message, output=output, debug=debug)


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
    # Serve the cached value stamped by the last backup/delete — running
    # `restic stats` on every request is slow enough to 504 behind the proxy
    # on large repos. `?refresh=1` bypasses the cache for a one-off live read
    # (used e.g. when the repo was changed out of band); it does not persist —
    # the stored cache updates only on the next backup or delete.
    force = request.args.get("refresh") in ("1", "true", "yes")
    if not force:
        cached = load_repo_stats_cache()
        if cached is not None:
            return jsonify(ok=True, stats=cached, cached=True)
    # No cache yet (fresh install, or first load before any backup) or a
    # forced refresh: compute live.
    stats, error = await repo_stats()
    if error:
        return jsonify(ok=False, error=error), 500
    return jsonify(ok=True, stats=stats, cached=False)


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
    logger.info(
        "chown -R %s:%s %s (skipping subuid-mapped files)",
        target_uid,
        target_gid,
        app_data,
    )

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

    logger.info(
        "chown complete: %d items fixed, %d skipped, %d errors", count, skipped, errors
    )
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
    idle = op_lock.idle_seconds() if op_lock.migration_running else None
    return jsonify(
        running=op_lock.migration_running,
        stale=idle is not None and idle > MIGRATION_IDLE_TIMEOUT_SECONDS,
        idle_seconds=round(idle) if idle is not None else None,
        status=migration.status,
        log=migration.log[-50:],
    )


# ---------------------------------------------------------------------------
# Direct push migration
# ---------------------------------------------------------------------------


@route("/api/migration/push", methods=["POST"])
async def trigger_direct_push():
    """One-click migration: push all apps + data to another instance."""
    _reclaim_abandoned_migration()
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
    # A previous migration whose source died can leave the lock held; reclaim it
    # so a retried migration proceeds without an app restart (issue #14).
    _reclaim_abandoned_migration()
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
    op_lock.touch()  # keep the migration lock fresh during a live transfer
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
    op_lock.touch()  # keep the migration lock fresh during a live transfer
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

    op_lock.touch()  # keep the migration lock fresh during a live transfer
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
