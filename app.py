import asyncio
import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from quart import Quart, Response, jsonify, render_template, request

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
}

# Snapshot IDs are hex strings; restic emits 8-char short IDs and 64-char long
# ones. Accept either (plus anything in between) for validation on API input.
SNAPSHOT_ID_RE = re.compile(r"^[a-f0-9]{8,64}$")


# Backups are tagged ``openhost`` (this app) plus ``zone:<domain>`` so that when
# several zones share one restic repo, each zone's snapshots — and its repo-size
# totals — stay distinguishable. When OPENHOST_ZONE_DOMAIN isn't set (local dev,
# tests) the zone tag is omitted and behaviour matches the old openhost-only
# scheme.
def _zone_tag() -> str | None:
    return f"zone:{ZONE_DOMAIN}" if ZONE_DOMAIN else None


def _backup_tags(name: str | None = None) -> list[str]:
    """Tags applied to a new snapshot: always ``openhost``, plus the zone tag
    (when known) and an optional ``name:<name>``."""
    tags = ["openhost"]
    zone = _zone_tag()
    if zone:
        tags.append(zone)
    if name:
        tags.append(f"name:{name}")
    return tags


def _snapshot_in_scope(tags: list[str]) -> bool:
    """Whether a snapshot belongs to this zone's view.

    In scope if it carries this zone's tag, OR carries no zone tag at all —
    the latter covers *legacy* snapshots written before zone tagging existed
    (and any run where OPENHOST_ZONE_DOMAIN wasn't set). Only snapshots tagged
    for a *different* zone are hidden. With no zone configured here, everything
    is in scope.

    restic ``--tag`` can't express "has no zone tag", so callers filter
    ``--tag openhost`` at restic and narrow with this in Python.
    """
    zone = _zone_tag()
    if zone is None:
        return True
    snapshot_zones = [t for t in tags if t.startswith("zone:")]
    return not snapshot_zones or zone in snapshot_zones


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

# Fire-and-forget background tasks (e.g. the post-delete repo-stats refresh).
# We keep a strong reference until each finishes — asyncio only holds a weak
# reference to running tasks, so without this a task can be garbage-collected
# mid-flight and silently cancelled. Each removes itself on completion.
_background_tasks: set[asyncio.Task] = set()


def _spawn_background(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


# ---------------------------------------------------------------------------
# Live operation-status push (Server-Sent Events)
# ---------------------------------------------------------------------------
# The status banner reflects op_lock state. Rather than relying only on the
# UI's slow poll, each connected browser holds an SSE stream (/api/events);
# op_lock's on_change callback wakes every stream so the banner updates the
# instant an operation starts or finishes.

# One queue per connected SSE client. A notification pushes a sentinel into
# each; the stream coroutine wakes, reads the current status, and emits it.
_status_subscribers: set[asyncio.Queue] = set()


def _lock_status() -> dict:
    """Current op_lock state — the payload the banner needs. Shared by
    /api/status and the SSE stream so the two never disagree."""
    active = op_lock.active
    return {
        "busy": op_lock.busy,
        "active_op": active.value if active else None,
        "busy_message": op_lock.busy_message(),
    }


def _notify_status_change() -> None:
    """Wake every SSE subscriber so it pushes the new status immediately.

    Runs synchronously from op_lock.try_acquire/release (same event-loop
    thread), so a non-blocking put_nowait is safe. A full queue already has a
    pending wake-up, so dropping the extra is fine.
    """
    for q in list(_status_subscribers):
        try:
            q.put_nowait(None)
        except asyncio.QueueFull:
            pass


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


def invalidate_repo_stats_cache() -> None:
    """Drop the cached repo-size stamp from every backup row.

    The cache describes a specific repository; call this when the configured
    repo changes so ``load_repo_stats_cache`` returns ``None`` and the next
    ``/api/repo/stats`` read computes live against the new repo instead of
    serving the old repo's size. Leaves the backup history itself untouched —
    only the auxiliary ``repo_stats_*`` columns are cleared.
    """
    conn = get_db()
    try:
        conn.execute(
            "UPDATE backups SET repo_size_bytes = NULL, "
            "repo_uncompressed_bytes = NULL, repo_blob_count = NULL, "
            "repo_snapshots_count = NULL, repo_compression_ratio = NULL, "
            "repo_stats_at = NULL WHERE repo_stats_at IS NOT NULL"
        )
        conn.commit()
    except sqlite3.Error:
        logger.exception("Failed to invalidate repo stats cache")
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
        # --no-lock: this is a pure read (existence/password probe), so it must
        # not take a restic repo lock — that keeps the invariant "op_lock is
        # held whenever a restic lock is held" true without gating this behind
        # op_lock, and lets it run even while a prune holds the exclusive lock.
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
        "--no-lock",  # pure read: never take a restic repo lock (see invariant)
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

    try:
        # Backup always creates the repo if missing — that's the operation
        # users opt into knowing it'll write to the configured location.
        init_err = (await ensure_repo_initialized(conf, auto_init=True))[1]
        if init_err:
            record_backup(timestamp, "error", init_err, name=name)
            logger.error("Backup failed: %s", init_err)
            return False

        tags = _backup_tags(name)

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
            # Backup succeeded. Compute the repo footprint now, while we
            # still hold the op lock (so the stats read stays serialized),
            # and fold it into the same row record_backup inserts — no
            # second connection or MAX(id) update. repo_stats() is
            # best-effort and never raises; on failure repo_stats_data is
            # None and the row just carries no fresh cache (the reader falls
            # back to the previous stamped row).
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
        # Filter to this app's snapshots (openhost) at restic, then narrow to
        # this zone in Python: keep this zone's snapshots plus legacy ones with
        # no zone tag, and drop snapshots belonging to a *different* zone. This
        # keeps pre-zone-tag snapshots readable while still isolating zones that
        # share a repo. See _snapshot_in_scope.
        rc, stdout, stderr = await _run_restic(
            ["snapshots", "--json", "--tag", "openhost", "--no-lock"],
            conf,
            timeout=60,
        )
        if rc != 0:
            logger.error(
                "restic snapshots failed: %s", stderr.decode(errors="replace").strip()
            )
            return [], False
        entries = json.loads(stdout.decode(errors="replace") or "[]")
        out = []
        for e in entries:
            tags = e.get("tags", []) or []
            if not _snapshot_in_scope(tags):
                continue
            out.append(
                {
                    "id": e.get("id", ""),
                    "short_id": e.get("short_id", ""),
                    "time": e.get("time", ""),
                    "paths": e.get("paths", []),
                    "tags": tags,
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
    compressed on-disk footprint of the repository (this is the number that
    matters for S3 cost / local disk usage). Scopes to the ``openhost`` tag —
    the *total* app footprint across zones. We intentionally don't narrow to a
    single zone here: restic dedups blobs across all snapshots, so per-zone
    size attribution is ill-defined, and the cost-relevant number is the whole
    openhost footprint (which also naturally includes legacy snapshots).
    """
    try:
        conf = load_config()
        if not conf.get("repo") or not conf.get("repo_password"):
            return None, "Restic repo not configured"
        # Auto-init only for local repos (see list_snapshots for the rationale).
        init_err = (await ensure_repo_initialized(conf))[1]
        if init_err:
            return None, init_err
        rc, stdout, stderr = await _run_restic(
            ["stats", "--mode", "raw-data", "--json", "--tag", "openhost", "--no-lock"],
            conf,
            timeout=60,
        )
        if rc != 0:
            return None, stderr.decode(errors="replace").strip() or f"restic exit {rc}"
        data = json.loads(stdout.decode(errors="replace") or "{}")
        # Returns stats on compressed binary blobs, not original content
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


async def _run_restic_streaming(
    args: list[str],
    conf: dict,
    timeout: float | None,
    on_line,
) -> tuple[int | None, bytes]:
    """Run ``restic <args>`` and feed each stdout line to ``on_line`` as it
    arrives, returning ``(returncode, stderr_bytes)``.

    Unlike ``_run_restic`` (which buffers all of stdout via ``communicate``),
    this reads stdout incrementally so a large ``restic ls`` doesn't
    materialise the whole recursive listing in memory — the caller keeps only
    what it needs. stderr is drained concurrently to avoid a pipe-buffer
    deadlock, and the subprocess is killed on timeout/cancellation so we don't
    leak a restic process holding the repo lock.
    """
    env = _restic_env(conf)
    proc = await asyncio.create_subprocess_exec(
        "restic",
        *args,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=2**20,  # allow long JSON lines (default readline limit is 64K)
    )
    stderr_buf = bytearray()

    async def _drain_stderr() -> None:
        assert proc.stderr is not None
        while True:
            chunk = await proc.stderr.read(4096)
            if not chunk:
                return
            stderr_buf.extend(chunk)

    async def _pump_stdout() -> None:
        assert proc.stdout is not None
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            on_line(line.decode(errors="replace"))
        await proc.wait()

    drain_task = asyncio.create_task(_drain_stderr())
    try:
        await asyncio.wait_for(_pump_stdout(), timeout=timeout)
        await asyncio.wait_for(drain_task, timeout=5)
    except BaseException:
        # Any failure — timeout, cancellation, or an on_line callback raising —
        # must still tear down the subprocess and drain task so we don't leak a
        # restic process holding the repo lock. Then re-raise unchanged.
        drain_task.cancel()
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        try:
            await proc.wait()
        except Exception:
            pass
        raise
    return proc.returncode, bytes(stderr_buf)


async def _roots_from_snapshot_metadata(
    snapshot_id: str, conf: dict
) -> list[dict] | None:
    """Which BACKUP_ROOTS a snapshot captured, read from its metadata.

    A snapshot records the absolute paths it backed up, so a single
    ``restic snapshots`` call tells us which roots are present — no recursive
    ``ls`` walk needed. Returns the present-root entries, or ``None`` if the
    metadata can't be read (the caller then falls back to probing).
    """
    try:
        rc, stdout, _stderr = await _run_restic(
            ["snapshots", "--json", snapshot_id, "--no-lock"], conf, timeout=60
        )
    except Exception:
        return None
    if rc != 0:
        return None
    try:
        entries = json.loads(stdout.decode(errors="replace") or "[]")
    except json.JSONDecodeError:
        return None
    if not isinstance(entries, list) or not entries:
        return None
    captured: set[str] = set()
    for e in entries:
        if not isinstance(e, dict):
            continue
        for p in e.get("paths", []) or []:
            captured.add(str(p).rstrip("/"))
    matched = [
        {"path": name, "size": 0, "is_dir": True, "mod_time": ""}
        for name, path in _ROOT_NAMES.items()
        if str(path).rstrip("/") in captured
    ]
    # A real snapshot always captures at least one root, so an empty match
    # means the metadata paths didn't line up as expected (e.g. a
    # normalization difference). Defer to the authoritative ls probe rather
    # than wrongly reporting "no roots".
    return matched or None


async def _list_roots_in_snapshot(snapshot_id: str, conf: dict):
    """Return the list of BACKUP_ROOTS actually present in this snapshot.

    A snapshot only contains roots that existed on disk at backup time. We
    read which ones from the snapshot's own metadata (cheap); only if that
    can't be read do we fall back to probing each root with ``restic ls``.
    """
    roots = await _roots_from_snapshot_metadata(snapshot_id, conf)
    if roots is not None:
        return roots
    present: list[dict] = []
    for name, path in _ROOT_NAMES.items():
        args = ["ls", "--json", snapshot_id, str(path), "--no-lock"]
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

    args = ["ls", "--json", snapshot_id, target_path, "--no-lock"]

    files: list[dict] = []
    target_norm = target_path.rstrip("/")

    def _collect(raw: str) -> None:
        raw = raw.strip()
        if not raw:
            return
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        if msg.get("struct_type") != "node":
            return
        path = msg.get("path", "")
        # Only immediate children of target_path.
        if not path.startswith(target_norm + "/"):
            # Could also be an exact match of the target (the dir itself) — skip.
            return
        rest = path[len(target_norm) + 1 :]
        if "/" in rest:
            return  # nested deeper, not a direct child
        files.append(
            {
                "path": rest,
                "size": msg.get("size", 0) or 0,
                "is_dir": msg.get("type") == "dir",
                "mod_time": msg.get("mtime", ""),
            }
        )

    # `restic ls` lists the whole subtree recursively; stream it line-by-line
    # and keep only the direct children rather than buffering the entire
    # listing (which can be huge/deep) in memory.
    try:
        rc, stderr = await _run_restic_streaming(args, conf, 120, _collect)
    except Exception as e:
        return [], f"restic error: {e}"

    if rc != 0:
        err = stderr.decode(errors="replace").strip()
        if "not found" in err.lower() or "no matching" in err.lower():
            return [], "Snapshot or path not found"
        return [], f"restic error: {err}"
    return files, None


async def delete_snapshot(snapshot_id: str) -> bool:
    """Remove a snapshot.

    Runs ``restic forget --prune`` so disk/object-store space is reclaimed
    immediately. Prune on a large repo can be slow (several minutes on an
    S3 repo with a lot of data) — we set a generous but bounded timeout so
    a wedged prune can't permanently hold the UI.

    Holds ``op_lock`` for its whole span (prune + DB cleanup + stats refresh)
    so a delete is a first-class operation: it shows in the status banner and
    mutually excludes backup/restore/migration. The route fires this via
    ``_spawn_background`` and returns immediately, so the prune runs in the
    background and the user tracks it through the banner.
    """
    conf = load_config()
    if not conf.get("repo") or not conf.get("repo_password"):
        return False

    err = op_lock.try_acquire(OpKind.DELETE)
    if err:
        logger.warning("Skipping delete: %s", err)
        return False
    try:
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

        # The prune reclaimed space, so the cached repo size is now stale.
        # Awaited inline (we hold the lock and the request has already
        # returned) so the banner stays up until the size is current.
        await _refresh_repo_stats_cache()

        logger.info("Deleted snapshot %s", snapshot_id)
        return True
    finally:
        op_lock.release(OpKind.DELETE)


async def _refresh_repo_stats_cache() -> None:
    """Recompute the repo footprint and re-stamp it onto the newest backup row.

    Runs as a background task after a delete/prune so the expensive
    ``restic stats`` read stays off the request path. Best-effort: on any
    failure (stats unavailable, no rows to stamp, DB error) the cache simply
    isn't refreshed and readers fall back to the previous stamped row.

    Unlike a backup (which folds its stats into its own INSERT), a delete has
    no row of its own, so we re-stamp the newest *surviving* row via MAX(id).
    """
    repo_stats_data = (await repo_stats())[0]
    if repo_stats_data is None:
        return
    conn = get_db()
    try:
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
        logger.exception("Failed to re-stamp repo stats cache after delete")
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

    Runs with ``--no-lock`` so the check takes no restic repo lock — it's a
    read-only integrity scan and can be long (up to 2h), so claiming the
    exclusive ``op_lock`` would needlessly block scheduled backups (which
    restic itself would let run concurrently). Because it holds no restic lock,
    the "op_lock held whenever a restic lock is held" invariant is satisfied
    without op_lock. Mutual exclusion is handled by the caller: the /api/check
    route rejects a check while another operation holds op_lock, and
    ``check_running`` prevents two checks at once.
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
                ["check", "--no-lock"], conf, timeout=CHECK_TIMEOUT_SECONDS
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
    # Push op_lock transitions to connected SSE clients so the status banner
    # updates the instant an operation starts or finishes.
    op_lock.set_on_change(_notify_status_change)
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

    # The cached repo size describes whatever repo was configured. If the repo
    # URL changes, that figure belongs to the old repo, so remember the old
    # value now (before we overwrite it) to invalidate the cache below.
    old_repo = current_conf.get("repo", "")

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
    save_config(conf)
    # Pointing at a different repo makes the cached size stale — drop it so the
    # next /api/repo/stats read computes live against the new repo.
    if conf.get("repo", "") != old_repo:
        invalidate_repo_stats_cache()
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
        return jsonify(ok=False, error=op_lock.busy_message()), 409
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
        delete_running=op_lock.delete_running,
        # Generic lock state so the UI can render one always-on banner for
        # whatever operation currently holds op_lock, without knowing each kind.
        **_lock_status(),
        last_backup=last["timestamp"] if last else None,
        last_status=last["status"] if last else None,
        last_error=last["error_message"] if last else None,
        interval_seconds=conf["interval_seconds"],
        repo=conf.get("repo", ""),
        backend=classify_repo(conf.get("repo", "")),
    )


@route("/api/events")
async def events():
    """Server-Sent Events stream of op_lock state.

    Emits the current status on connect, then again on every lock transition
    (pushed via ``_notify_status_change``), with a periodic comment keepalive
    so idle connections survive proxies. The UI uses this to update the banner
    instantly; its slow poll is only a fallback.
    """

    async def stream():
        q: asyncio.Queue = asyncio.Queue()
        _status_subscribers.add(q)
        try:
            yield f"data: {json.dumps(_lock_status())}\n\n"
            while True:
                try:
                    await asyncio.wait_for(q.get(), timeout=25)
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
                    continue
                # Coalesce a burst of notifications into one status emit.
                while not q.empty():
                    q.get_nowait()
                yield f"data: {json.dumps(_lock_status())}\n\n"
        finally:
            _status_subscribers.discard(q)

    return Response(
        stream(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            # Disable proxy buffering (nginx) so events aren't held back.
            "X-Accel-Buffering": "no",
        },
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
        return jsonify(ok=False, error=op_lock.busy_message()), 409
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
        return jsonify(ok=False, error=op_lock.busy_message()), 409
    # Run the prune in the background (it can take minutes) and return
    # immediately. delete_snapshot acquires op_lock(DELETE), so the status
    # banner reflects it and other operations get a clean busy rejection; the
    # UI watches the banner and refreshes the snapshot list when it clears.
    _spawn_background(delete_snapshot(snapshot_id))
    return jsonify(ok=True, message="Delete started")


@route("/api/check", methods=["POST"])
async def trigger_check():
    # run_check runs `restic check --no-lock`, so it holds no repo lock and
    # doesn't claim op_lock
    if op_lock.busy:
        return jsonify(ok=False, error=op_lock.busy_message()), 409
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
