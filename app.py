print("backup app module loading...", flush=True)

import asyncio
import json
import logging
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from quart import Quart, render_template, request, jsonify

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
logger.info("backup app module loaded")

app = Quart(__name__)

BASE_PATH = os.environ.get("OPENHOST_APP_BASE_PATH", "/backup")
APP_DATA_DIR = Path(os.environ.get("OPENHOST_APP_DATA_DIR", "/data/app_data/backup"))
ALL_APP_DATA = Path("/data/app_data")

CONFIG_DIR = APP_DATA_DIR
RCLONE_CONF = CONFIG_DIR / "rclone.conf"
CONFIG_FILE = CONFIG_DIR / "config.json"
DB_FILE = CONFIG_DIR / "backups.db"

DEFAULT_CONFIG = {
    "interval_seconds": 3600,
    "remote_name": "openhost-backup",
    "remote_path": "",
}


def init_db():
    """Create tables if they don't exist. Call once at startup."""
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
            size_bytes INTEGER,
            file_count INTEGER,
            name TEXT
        )
    """)
    # Migrate existing tables that lack the new columns
    cursor = conn.execute("PRAGMA table_info(backups)")
    columns = {row[1] for row in cursor.fetchall()}
    if "size_bytes" not in columns:
        conn.execute("ALTER TABLE backups ADD COLUMN size_bytes INTEGER")
    if "file_count" not in columns:
        conn.execute("ALTER TABLE backups ADD COLUMN file_count INTEGER")
    if "name" not in columns:
        conn.execute("ALTER TABLE backups ADD COLUMN name TEXT")
    conn.commit()
    conn.close()


def get_db():
    """Get a database connection."""
    return sqlite3.connect(str(DB_FILE))


def record_backup(timestamp, status, error_message=None, size_bytes=None, file_count=None, name=None):
    """Insert a backup record into the database."""
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO backups (timestamp, status, error_message, size_bytes, file_count, name) VALUES (?, ?, ?, ?, ?, ?)",
            (timestamp, status, error_message, size_bytes, file_count, name),
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


# In-memory flags — not persisted (reset on startup is correct behavior)
backup_running = False
restore_running = False
restore_last_snapshot = None
restore_last_status = None

scheduler_task = None


def load_config():
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            saved = json.load(f)
        conf = {**DEFAULT_CONFIG, **saved}
        return conf
    return dict(DEFAULT_CONFIG)


def save_config(conf):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        json.dump(conf, f, indent=2)


def load_rclone_conf():
    if RCLONE_CONF.exists():
        return RCLONE_CONF.read_text()
    return ""


def save_rclone_conf(text):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    RCLONE_CONF.write_text(text)


def generate_s3_conf(remote_name, bucket, access_key, secret_key, region):
    return (
        f"[{remote_name}]\n"
        f"type = s3\n"
        f"provider = AWS\n"
        f"access_key_id = {access_key}\n"
        f"secret_access_key = {secret_key}\n"
        f"region = {region}\n"
    )


async def run_backup(name=None):
    global backup_running
    if backup_running:
        logger.warning("Backup already in progress, skipping")
        return False

    conf = load_config()
    remote_name = conf["remote_name"]
    remote_path = conf["remote_path"]

    if not RCLONE_CONF.exists():
        logger.error("No rclone.conf configured, skipping backup")
        return False

    if not remote_name or not remote_path:
        logger.error("Remote name or path not configured, skipping backup")
        return False

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    dest = f"{remote_name}:{remote_path}/{timestamp}"

    backup_running = True
    logger.info("Starting backup to %s", dest)

    try:
        proc = await asyncio.create_subprocess_exec(
            "rclone", "copy",
            str(ALL_APP_DATA),
            dest,
            "--config", str(RCLONE_CONF),
            "--exclude", "backup/**",
            "-v",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        stdout, _ = await proc.communicate()
        if stdout:
            for line in stdout.decode().splitlines():
                logger.info("rclone: %s", line)

        if proc.returncode == 0:
            # Record size from the just-completed backup
            size_bytes = None
            file_count = None
            try:
                size_info = await get_snapshot_size(timestamp)
                size_bytes = size_info["bytes"]
                file_count = size_info["count"]
            except Exception:
                logger.warning("Could not get backup size, recording without it")
            record_backup(timestamp, "success", size_bytes=size_bytes, file_count=file_count, name=name)
            logger.info("Backup completed successfully")
            return True
        else:
            error_msg = f"rclone exit code {proc.returncode}"
            record_backup(timestamp, "error", error_msg)
            logger.error("Backup failed with exit code %d", proc.returncode)
            return False
    except Exception as e:
        record_backup(timestamp, "error", str(e))
        logger.exception("Backup failed")
        return False
    finally:
        backup_running = False


SNAPSHOT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$")


async def list_snapshots():
    """List available backup snapshots from the remote.

    Returns (names, ok) where ok indicates if the remote was reachable.
    """
    conf = load_config()
    remote_name = conf["remote_name"]
    remote_path = conf["remote_path"]

    if not RCLONE_CONF.exists() or not remote_name or not remote_path:
        return [], False

    try:
        proc = await asyncio.create_subprocess_exec(
            "rclone", "lsjson",
            f"{remote_name}:{remote_path}",
            "--config", str(RCLONE_CONF),
            "--dirs-only",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.error("Failed to list snapshots: %s", stderr.decode())
            return [], False

        entries = json.loads(stdout.decode())
        names = sorted(
            [e["Path"] for e in entries if e.get("IsDir")],
            reverse=True,
        )
        return names, True
    except Exception as e:
        logger.exception("Failed to list snapshots")
        return [], False


def validate_subpath(path):
    """Validate a browse subpath to prevent directory traversal."""
    if not path:
        return True
    segments = path.split("/")
    for seg in segments:
        if seg == ".." or seg == "." or not seg:
            return False
        if not re.match(r"^[\w\-:. ]+$", seg):
            return False
    return True


async def get_snapshot_size(snapshot):
    """Get total size and file count for a snapshot."""
    conf = load_config()
    remote_name = conf["remote_name"]
    remote_path = conf["remote_path"]

    proc = await asyncio.create_subprocess_exec(
        "rclone", "size", "--json",
        f"{remote_name}:{remote_path}/{snapshot}",
        "--config", str(RCLONE_CONF),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"rclone size failed: {stderr.decode().strip()}")

    data = json.loads(stdout.decode())
    return {"bytes": data.get("bytes", 0), "count": data.get("count", 0)}


async def list_snapshot_files(snapshot, subpath=""):
    """List files and directories within a snapshot.

    Returns (files, error) where error is a user-friendly string or None.
    """
    conf = load_config()
    remote_name = conf["remote_name"]
    remote_path = conf["remote_path"]

    target = f"{remote_name}:{remote_path}/{snapshot}"
    if subpath:
        target += f"/{subpath}"

    proc = await asyncio.create_subprocess_exec(
        "rclone", "lsjson",
        target,
        "--config", str(RCLONE_CONF),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        err = stderr.decode().strip()
        if "directory not found" in err or "not found" in err.lower():
            return [], "Snapshot not found on remote"
        return [], f"rclone error: {err}"

    entries = json.loads(stdout.decode())
    return [
        {
            "path": e["Path"],
            "size": e.get("Size", 0),
            "is_dir": e.get("IsDir", False),
            "mod_time": e.get("ModTime", ""),
        }
        for e in entries
    ], None


async def delete_snapshot(snapshot):
    """Delete a snapshot from remote storage and the local DB record."""
    conf = load_config()
    remote_name = conf["remote_name"]
    remote_path = conf["remote_path"]

    remote_deleted = False
    if RCLONE_CONF.exists() and remote_name and remote_path:
        try:
            proc = await asyncio.create_subprocess_exec(
                "rclone", "purge",
                f"{remote_name}:{remote_path}/{snapshot}",
                "--config", str(RCLONE_CONF),
                "--contimeout", "10s",
                "--timeout", "30s",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=45)
            if proc.returncode == 0:
                remote_deleted = True
                logger.info("Deleted snapshot %s from remote", snapshot)
            else:
                err = stderr.decode().strip()
                if "not found" in err.lower() or "directory not found" in err:
                    remote_deleted = True
                    logger.info("Snapshot %s already gone from remote", snapshot)
                else:
                    logger.warning("rclone purge failed for %s: %s", snapshot, err)
        except asyncio.TimeoutError:
            logger.warning("rclone purge timed out for %s", snapshot)

    # Always remove the DB record
    conn = get_db()
    try:
        conn.execute("DELETE FROM backups WHERE timestamp = ?", (snapshot,))
        conn.commit()
    finally:
        conn.close()
    logger.info("Deleted DB record for snapshot %s", snapshot)

    return remote_deleted


def get_backup_history(limit=20, offset=0):
    """Get paginated backup history from the database."""
    conn = get_db()
    try:
        total = conn.execute("SELECT COUNT(*) FROM backups").fetchone()[0]
        rows = conn.execute(
            "SELECT id, timestamp, status, error_message, created_at, size_bytes, file_count, name FROM backups ORDER BY id DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
        history = [
            {
                "id": r[0],
                "timestamp": r[1],
                "status": r[2],
                "error_message": r[3],
                "created_at": r[4],
                "size_bytes": r[5],
                "file_count": r[6],
                "name": r[7],
            }
            for r in rows
        ]
        return history, total
    finally:
        conn.close()


def get_backup_sizes():
    """Get a map of timestamp -> {size_bytes, file_count} for all successful backups."""
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT timestamp, size_bytes, file_count FROM backups WHERE status = 'success' AND size_bytes IS NOT NULL"
        ).fetchall()
        return {r[0]: {"size_bytes": r[1], "file_count": r[2]} for r in rows}
    finally:
        conn.close()


async def run_restore(snapshot):
    """Restore app data from a remote backup snapshot."""
    global restore_running, restore_last_snapshot, restore_last_status
    if restore_running:
        logger.warning("Restore already in progress, skipping")
        return False

    if backup_running:
        logger.warning("Backup in progress, cannot restore")
        return False

    conf = load_config()
    remote_name = conf["remote_name"]
    remote_path = conf["remote_path"]

    if not RCLONE_CONF.exists():
        restore_last_status = "error: no rclone.conf"
        return False

    if not remote_name or not remote_path:
        restore_last_status = "error: remote not configured"
        return False

    if not SNAPSHOT_RE.match(snapshot):
        restore_last_status = "error: invalid snapshot name"
        return False

    src = f"{remote_name}:{remote_path}/{snapshot}"
    restore_running = True
    logger.info("Starting restore from %s", src)

    try:
        proc = await asyncio.create_subprocess_exec(
            "rclone", "copy",
            src,
            str(ALL_APP_DATA),
            "--config", str(RCLONE_CONF),
            "--exclude", "backup/**",
            "-v",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        stdout, _ = await proc.communicate()
        if stdout:
            for line in stdout.decode().splitlines():
                logger.info("rclone: %s", line)

        if proc.returncode == 0:
            restore_last_snapshot = snapshot
            restore_last_status = "success"
            logger.info("Restore completed successfully")
        else:
            restore_last_status = f"error: rclone exit code {proc.returncode}"
            logger.error("Restore failed with exit code %d", proc.returncode)
    except Exception as e:
        restore_last_status = f"error: {e}"
        logger.exception("Restore failed")
    finally:
        restore_running = False

    return restore_last_status == "success"


async def scheduler_loop():
    # On first iteration, account for time already elapsed since last backup
    first_run = True
    while True:
        conf = load_config()
        interval = conf["interval_seconds"]

        if first_run:
            first_run = False
            last = get_last_backup()
            if last and last["timestamp"]:
                try:
                    last_dt = datetime.strptime(last["timestamp"], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
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


@app.before_serving
async def startup():
    global scheduler_task
    init_db()
    scheduler_task = asyncio.create_task(scheduler_loop())
    logger.info("Backup scheduler started")


@app.after_serving
async def shutdown():
    if scheduler_task:
        scheduler_task.cancel()


def route(path, **kwargs):
    """Register a route at both /path and BASE_PATH/path to handle proxies."""
    def decorator(func):
        app.route(path, **kwargs)(func)
        if BASE_PATH and BASE_PATH != "/":
            prefixed = BASE_PATH.rstrip("/") + path
            app.route(prefixed, **kwargs)(func)
        return func
    return decorator


@route("/")
async def index():
    conf = load_config()
    rclone_conf = load_rclone_conf()
    last = get_last_backup()
    state = {
        "running": backup_running,
        "last_backup": last["timestamp"] if last else None,
        "last_status": last["status"] if last else None,
        "last_error": last["error_message"] if last else None,
    }
    return await render_template(
        "index.html",
        base_path=BASE_PATH,
        config=conf,
        rclone_conf=rclone_conf,
        state=state,
    )


@route("/api/config", methods=["GET"])
async def get_config():
    return jsonify(config=load_config(), rclone_conf=load_rclone_conf())


@route("/api/config", methods=["POST"])
async def post_config():
    data = await request.get_json()

    if "rclone_conf" in data:
        save_rclone_conf(data["rclone_conf"])

    conf = load_config()
    for key in ("interval_seconds", "remote_name", "remote_path"):
        if key in data:
            conf[key] = data[key]
    if "interval_seconds" in data:
        conf["interval_seconds"] = max(60, int(conf["interval_seconds"]))
    save_config(conf)

    return jsonify(ok=True)


@route("/api/setup-s3", methods=["POST"])
async def setup_s3():
    data = await request.get_json()
    remote_name = data.get("remote_name", "openhost-backup")
    bucket = data.get("bucket", "")
    access_key = data.get("access_key", "")
    secret_key = data.get("secret_key", "")
    region = data.get("region", "us-east-1")

    rclone_text = generate_s3_conf(remote_name, bucket, access_key, secret_key, region)
    save_rclone_conf(rclone_text)

    conf = load_config()
    conf["remote_name"] = remote_name
    conf["remote_path"] = bucket
    save_config(conf)

    return jsonify(ok=True, rclone_conf=rclone_text)


@route("/api/backup", methods=["POST"])
async def trigger_backup():
    if backup_running:
        return jsonify(ok=False, error="Backup already in progress"), 409
    data = await request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip() or None
    asyncio.create_task(run_backup(name=name))
    return jsonify(ok=True, message="Backup started")


@route("/api/status")
async def status():
    conf = load_config()
    last = get_last_backup()
    return jsonify(
        running=backup_running,
        last_backup=last["timestamp"] if last else None,
        last_status=last["status"] if last else None,
        last_error=last["error_message"] if last else None,
        interval_seconds=conf["interval_seconds"],
    )


@route("/api/backups")
async def get_backups():
    snapshots, remote_ok = await list_snapshots()
    sizes = get_backup_sizes()
    enriched = []
    for name in snapshots:
        entry = {"name": name}
        if name in sizes:
            entry["size_bytes"] = sizes[name]["size_bytes"]
            entry["file_count"] = sizes[name]["file_count"]
        enriched.append(entry)
    return jsonify(snapshots=enriched, remote_ok=remote_ok)


@route("/api/restore", methods=["POST"])
async def trigger_restore():
    if restore_running:
        return jsonify(ok=False, error="Restore already in progress"), 409
    if backup_running:
        return jsonify(ok=False, error="Backup in progress, cannot restore"), 409

    data = await request.get_json()
    snapshot = data.get("snapshot", "")
    if not snapshot or not SNAPSHOT_RE.match(snapshot):
        return jsonify(ok=False, error="Invalid snapshot name"), 400

    asyncio.create_task(run_restore(snapshot))
    return jsonify(ok=True, message="Restore started")


@route("/api/restore/status")
async def restore_status():
    return jsonify(
        running=restore_running,
        last_restore=restore_last_snapshot,
        last_status=restore_last_status,
    )


@route("/api/snapshot/files")
async def snapshot_files():
    name = request.args.get("snapshot", "")
    if not name or not SNAPSHOT_RE.match(name):
        return jsonify(ok=False, error="Invalid snapshot name"), 400
    subpath = request.args.get("path", "")
    if not validate_subpath(subpath):
        return jsonify(ok=False, error="Invalid path"), 400
    try:
        files, error = await list_snapshot_files(name, subpath)
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
    name = data.get("snapshot", "")
    if not name or not SNAPSHOT_RE.match(name):
        return jsonify(ok=False, error="Invalid snapshot name"), 400
    if backup_running:
        return jsonify(ok=False, error="Backup in progress"), 409
    if restore_running:
        return jsonify(ok=False, error="Restore in progress"), 409
    try:
        remote_deleted = await delete_snapshot(name)
        return jsonify(ok=True, remote_deleted=remote_deleted)
    except Exception as e:
        logger.exception("Failed to delete snapshot")
        return jsonify(ok=False, error=str(e)), 500


@route("/api/history")
async def backup_history():
    limit = min(int(request.args.get("limit", 20)), 100)
    offset = int(request.args.get("offset", 0))
    history, total = get_backup_history(limit, offset)
    return jsonify(ok=True, history=history, total=total)


@route("/api/local/files")
async def local_files():
    """Browse the current local app data (the backup source)."""
    subpath = request.args.get("path", "")
    if not validate_subpath(subpath):
        return jsonify(ok=False, error="Invalid path"), 400

    target = ALL_APP_DATA / subpath if subpath else ALL_APP_DATA
    if not target.exists() or not target.is_dir():
        return jsonify(ok=False, error="Directory not found"), 404

    # Prevent traversal outside ALL_APP_DATA
    try:
        target.resolve().relative_to(ALL_APP_DATA.resolve())
    except ValueError:
        return jsonify(ok=False, error="Invalid path"), 400

    files = []
    try:
        for entry in sorted(target.iterdir(), key=lambda e: (not e.is_dir(), e.name)):
            # Skip the backup app's own data directory
            rel = entry.relative_to(ALL_APP_DATA)
            if str(rel).startswith("backup"):
                continue
            stat = entry.stat()
            files.append({
                "path": entry.name,
                "size": stat.st_size if entry.is_file() else 0,
                "is_dir": entry.is_dir(),
                "mod_time": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
            })
    except PermissionError:
        return jsonify(ok=False, error="Permission denied"), 403

    return jsonify(ok=True, files=files)


@route("/api/backup/rename", methods=["POST"])
async def rename_backup():
    """Set or update the name for a backup entry."""
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


@route("/health")
async def health():
    return "ok"
