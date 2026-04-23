import asyncio
import json
import logging
import os
import re
import secrets as pysecrets
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
VM_DATA_DIR = Path("/data/vm_data")
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
}

# Snapshot IDs are hex strings; restic emits 8-char short IDs and 64-char long
# ones. Accept either (plus anything in between) for validation on API input.
SNAPSHOT_ID_RE = re.compile(r"^[a-f0-9]{8,64}$")

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
    return env


async def _run_restic(args: list[str], conf: dict, timeout: float | None = None):
    """Run `restic <args>` with configured repo, return (returncode, stdout, stderr)."""
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
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise
    return proc.returncode, stdout, stderr


async def ensure_repo_initialized(conf: dict) -> tuple[bool, str | None]:
    """Ensure the restic repo exists; run `restic init` if not.

    Returns (initialized_now, error_message).
    """
    # `cat config` is a cheap way to confirm the repo exists and the password
    # is correct. It returns non-zero on either missing repo or wrong password.
    rc, _stdout, stderr = await _run_restic(["cat", "config"], conf, timeout=30)
    if rc == 0:
        return False, None

    err = stderr.decode(errors="replace").strip()
    # If the repo simply doesn't exist, init it. Heuristic on the error text;
    # restic doesn't expose a clean "not found" exit code.
    if "does not exist" in err.lower() or "unable to open config" in err.lower() or "no such file" in err.lower():
        # Local repo path: make sure parent exists.
        repo = conf["repo"]
        if not repo.startswith(("s3:", "b2:", "sftp:", "rest:", "gs:", "azure:", "swift:", "rclone:")):
            Path(repo).parent.mkdir(parents=True, exist_ok=True)
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
        init_err = (await ensure_repo_initialized(conf))[1]
        if init_err:
            record_backup(timestamp, "error", init_err, name=name)
            logger.error("Backup failed: %s", init_err)
            return False

        tags = ["openhost"]
        if name:
            tags.append(f"name:{name}")

        args = [
            "backup",
            str(ALL_APP_DATA),
            "--exclude",
            str(ALL_APP_DATA / "backup"),
            "--json",
        ]
        for t in tags:
            args += ["--tag", t]

        env = _restic_env(conf)
        proc = await asyncio.create_subprocess_exec(
            "restic",
            *args,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()

        summary = None
        if stdout:
            # Restic emits NDJSON to stdout with --json; the last `summary`
            # message contains the snapshot ID and byte counts.
            for raw in stdout.decode(errors="replace").splitlines():
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    logger.info("restic: %s", raw)
                    continue
                if msg.get("message_type") == "summary":
                    summary = msg

        if stderr:
            for line in stderr.decode(errors="replace").splitlines():
                if line.strip():
                    logger.info("restic stderr: %s", line)

        if proc.returncode == 0 and summary is not None:
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

        if proc.returncode == 0:
            # Succeeded but we somehow missed the summary line.
            record_backup(timestamp, "success", name=name)
            logger.info("Backup completed (no summary parsed)")
            return True

        error_msg = stderr.decode(errors="replace").strip() or f"restic exit code {proc.returncode}"
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
    try:
        rc, stdout, stderr = await _run_restic(
            ["snapshots", "--json"], conf, timeout=60
        )
        if rc != 0:
            logger.error("restic snapshots failed: %s", stderr.decode(errors="replace").strip())
            return [], False
        entries = json.loads(stdout.decode() or "[]")
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


def validate_subpath(path: str) -> bool:
    if not path:
        return True
    for seg in path.split("/"):
        if seg in ("..", ".") or not seg:
            return False
        if not re.match(r"^[\w\-:. ]+$", seg):
            return False
    return True


async def list_snapshot_files(snapshot_id: str, subpath: str = ""):
    """List files in a snapshot at the given subpath.

    Returns (files, error). `subpath` is relative to ``ALL_APP_DATA``.
    """
    conf = load_config()
    if not conf.get("repo") or not conf.get("repo_password"):
        return [], "Restic repo not configured"

    # `restic ls --json <id> <path>` emits NDJSON: one "snapshot" header then
    # one "node" per entry. We want entries that are direct children of
    # `target_path`.
    target_path = str(ALL_APP_DATA)
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
    """Remove a snapshot. Runs `forget`; prune happens separately / later."""
    conf = load_config()
    if not conf.get("repo") or not conf.get("repo_password"):
        return False
    try:
        rc, _out, stderr = await _run_restic(
            ["forget", snapshot_id], conf, timeout=120
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

    conn = get_db()
    try:
        conn.execute(
            "DELETE FROM backups WHERE snapshot_id = ? OR snapshot_id LIKE ?",
            (snapshot_id, snapshot_id + "%"),
        )
        conn.commit()
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


async def run_restore(snapshot_id: str) -> bool:
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

    logger.info("Starting restic restore from %s", snapshot_id)

    try:
        args = [
            "restore",
            snapshot_id,
            "--target",
            "/",  # restic restores the absolute paths as they were captured
            "--exclude",
            str(ALL_APP_DATA / "backup"),
        ]
        rc, _stdout, stderr = await _run_restic(args, conf, timeout=None)

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
    """Run `restic check`. Updates module-level state. Doesn't take op lock —
    check is read-only; restic takes its own repo lock."""
    global check_last_status, check_last_output, check_last_at, check_running
    check_running = True
    conf = load_config()
    if not conf.get("repo") or not conf.get("repo_password"):
        check_last_status = "error"
        check_last_output = "Restic repo not configured"
        check_last_at = datetime.now(timezone.utc).isoformat()
        check_running = False
        return False
    try:
        rc, stdout, stderr = await _run_restic(["check"], conf, timeout=None)
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
        conf["repo_password"] = pysecrets.token_urlsafe(32)
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
    return await render_template(
        "index.html",
        base_path=BASE_PATH,
        config=conf,
        state=state,
    )


@route("/api/config", methods=["GET"])
async def get_config():
    conf = load_config()
    # Only return password once the user explicitly asks; keep it out of the
    # default GET so it doesn't leak via browser caches / history.
    redacted = {**conf, "repo_password": "***" if conf.get("repo_password") else ""}
    return jsonify(config=redacted)


@route("/api/config", methods=["POST"])
async def post_config():
    data = await request.get_json()
    conf = load_config()
    for key in ("interval_seconds", "repo", "repo_password", "router_api_token"):
        if key in data and data[key] not in (None, "***"):
            conf[key] = data[key]
    if "interval_seconds" in data:
        conf["interval_seconds"] = max(60, int(conf["interval_seconds"]))
    save_config(conf)
    return jsonify(ok=True)


@route("/api/config/password", methods=["GET"])
async def reveal_password():
    """Return the actual repo password. Separate endpoint so it doesn't leak
    into the default config response / UI state dumps."""
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
    )


@route("/api/snapshots")
async def api_snapshots():
    snapshots, repo_ok = await list_snapshots()
    return jsonify(ok=True, snapshots=snapshots, repo_ok=repo_ok)


@route("/api/restore", methods=["POST"])
async def trigger_restore():
    if op_lock.busy:
        return jsonify(ok=False, error=f"{op_lock.active.value} in progress"), 409
    data = await request.get_json()
    snapshot_id = data.get("snapshot", "")
    if not snapshot_id or not SNAPSHOT_ID_RE.match(snapshot_id):
        return jsonify(ok=False, error="Invalid snapshot id"), 400
    asyncio.create_task(run_restore(snapshot_id))
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
    subpath = request.args.get("path", "")
    if not validate_subpath(subpath):
        return jsonify(ok=False, error="Invalid path"), 400
    try:
        files, error = await list_snapshot_files(snapshot_id, subpath)
        if error:
            status_code = 404 if "not found" in error.lower() else 500
            return jsonify(ok=False, error=error), status_code
        return jsonify(ok=True, files=files)
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
    # acquire a repo lock, so don't run it on top of a backup/restore.
    if op_lock.busy:
        return jsonify(ok=False, error=f"{op_lock.active.value} in progress"), 409
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


@route("/api/chown-app-data", methods=["POST"])
async def chown_app_data():
    """Recursively chown all app_data to match the host user.

    Only allowed when all non-backup apps are stopped, to prevent
    ownership changes on files being actively written.
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
    logger.info("chown -R %s:%s %s", target_uid, target_gid, app_data)

    count = 0
    errors = 0
    for root, dirs, files in os.walk(app_data):
        for name in dirs + files:
            path = os.path.join(root, name)
            try:
                os.chown(path, target_uid, target_gid)
                count += 1
            except OSError as e:
                errors += 1
                logger.warning("chown failed for %s: %s", path, e)
    try:
        os.chown(app_data, target_uid, target_gid)
        count += 1
    except OSError:
        errors += 1

    logger.info("chown complete: %d items fixed, %d errors", count, errors)
    return jsonify(
        ok=True,
        message=f"Ownership fixed on {count} items (uid={target_uid}, gid={target_gid})",
        count=count,
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
