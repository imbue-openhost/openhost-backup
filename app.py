import asyncio
import json
import logging
import os
import re
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

# ---------------------------------------------------------------------------
# Paths & configuration
# ---------------------------------------------------------------------------

BASE_PATH = os.environ.get("OPENHOST_APP_BASE_PATH", "/backup")
APP_DATA_DIR = Path(os.environ.get("OPENHOST_APP_DATA_DIR", "/data/app_data/backup"))
ALL_APP_DATA = Path("/data/app_data")
VM_DATA_DIR = Path("/data/vm_data")
ROUTER_URL = os.environ.get("OPENHOST_ROUTER_URL", "http://host.docker.internal:8080")
ZONE_DOMAIN = os.environ.get("OPENHOST_ZONE_DOMAIN", "")

CONFIG_DIR = APP_DATA_DIR
RCLONE_CONF = CONFIG_DIR / "rclone.conf"
CONFIG_FILE = CONFIG_DIR / "config.json"
DB_FILE = CONFIG_DIR / "backups.db"
LOCAL_SNAPSHOTS_DIR = APP_DATA_DIR / "snapshots"

DEFAULT_REMOTE_NAME = "local-backup"
DEFAULT_REMOTE_PATH = str(LOCAL_SNAPSHOTS_DIR)

DEFAULT_CONFIG = {
    "interval_seconds": 3600,
    "remote_name": DEFAULT_REMOTE_NAME,
    "remote_path": DEFAULT_REMOTE_PATH,
}

# Single lock for mutual exclusion across backup / restore / migration.
op_lock = OperationLock()

# Restore-specific status (not part of the lock itself).
restore_last_snapshot = None
restore_last_status = None

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
            size_bytes INTEGER,
            file_count INTEGER,
            name TEXT
        )
    """)
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
    return sqlite3.connect(str(DB_FILE))


def record_backup(
    timestamp, status, error_message=None, size_bytes=None, file_count=None, name=None
):
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


# ---------------------------------------------------------------------------
# Backup
# ---------------------------------------------------------------------------


async def run_backup(name=None):
    err = op_lock.try_acquire(OpKind.BACKUP)
    if err:
        logger.warning("Skipping backup: %s", err)
        return False

    conf = load_config()
    remote_name = conf["remote_name"]
    remote_path = conf["remote_path"]

    if not RCLONE_CONF.exists():
        logger.error("No rclone.conf configured, skipping backup")
        op_lock.release(OpKind.BACKUP)
        return False

    if not remote_name or not remote_path:
        logger.error("Remote name or path not configured, skipping backup")
        op_lock.release(OpKind.BACKUP)
        return False

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    dest = f"{remote_name}:{remote_path}/{timestamp}"
    logger.info("Starting backup to %s", dest)

    try:
        proc = await asyncio.create_subprocess_exec(
            "rclone", "copy", str(ALL_APP_DATA), dest,
            "--config", str(RCLONE_CONF),
            "--exclude", "backup/**", "-v",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        stdout, _ = await proc.communicate()
        if stdout:
            for line in stdout.decode().splitlines():
                logger.info("rclone: %s", line)

        if proc.returncode == 0:
            size_bytes = file_count = None
            try:
                size_info = await get_snapshot_size(timestamp)
                size_bytes = size_info["bytes"]
                file_count = size_info["count"]
            except Exception:
                logger.warning("Could not get backup size, recording without it")
            record_backup(timestamp, "success", size_bytes=size_bytes,
                          file_count=file_count, name=name)
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
        op_lock.release(OpKind.BACKUP)


# ---------------------------------------------------------------------------
# Snapshot helpers
# ---------------------------------------------------------------------------

SNAPSHOT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$")


async def list_snapshots():
    conf = load_config()
    remote_name = conf["remote_name"]
    remote_path = conf["remote_path"]

    if not RCLONE_CONF.exists() or not remote_name or not remote_path:
        return [], False

    try:
        proc = await asyncio.create_subprocess_exec(
            "rclone", "lsjson", f"{remote_name}:{remote_path}",
            "--config", str(RCLONE_CONF), "--dirs-only",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.error("Failed to list snapshots: %s", stderr.decode())
            return [], False

        entries = json.loads(stdout.decode())
        names = sorted(
            [e["Path"] for e in entries
             if e.get("IsDir") and e["Path"] != "migrations"],
            reverse=True,
        )
        return names, True
    except Exception:
        logger.exception("Failed to list snapshots")
        return [], False


def validate_subpath(path):
    if not path:
        return True
    for seg in path.split("/"):
        if seg in ("..", ".") or not seg:
            return False
        if not re.match(r"^[\w\-:. ]+$", seg):
            return False
    return True


async def get_snapshot_size(snapshot):
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
    conf = load_config()
    remote_name = conf["remote_name"]
    remote_path = conf["remote_path"]

    target = f"{remote_name}:{remote_path}/{snapshot}"
    if subpath:
        target += f"/{subpath}"

    proc = await asyncio.create_subprocess_exec(
        "rclone", "lsjson", target,
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
                "--contimeout", "10s", "--timeout", "30s",
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

    conn = get_db()
    try:
        conn.execute("DELETE FROM backups WHERE timestamp = ?", (snapshot,))
        conn.commit()
    finally:
        conn.close()
    logger.info("Deleted DB record for snapshot %s", snapshot)
    return remote_deleted


def get_backup_history(limit=20, offset=0):
    conn = get_db()
    try:
        total = conn.execute("SELECT COUNT(*) FROM backups").fetchone()[0]
        rows = conn.execute(
            "SELECT id, timestamp, status, error_message, created_at, size_bytes, file_count, name "
            "FROM backups ORDER BY id DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
        history = [
            {
                "id": r[0], "timestamp": r[1], "status": r[2],
                "error_message": r[3], "created_at": r[4],
                "size_bytes": r[5], "file_count": r[6], "name": r[7],
            }
            for r in rows
        ]
        return history, total
    finally:
        conn.close()


def get_backup_sizes():
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT timestamp, size_bytes, file_count FROM backups "
            "WHERE status = 'success' AND size_bytes IS NOT NULL"
        ).fetchall()
        return {r[0]: {"size_bytes": r[1], "file_count": r[2]} for r in rows}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Restore
# ---------------------------------------------------------------------------


async def run_restore(snapshot):
    global restore_last_snapshot, restore_last_status

    err = op_lock.try_acquire(OpKind.RESTORE)
    if err:
        logger.warning("Skipping restore: %s", err)
        return False

    conf = load_config()
    remote_name = conf["remote_name"]
    remote_path = conf["remote_path"]

    if not RCLONE_CONF.exists():
        restore_last_status = "error: no rclone.conf"
        op_lock.release(OpKind.RESTORE)
        return False

    if not remote_name or not remote_path:
        restore_last_status = "error: remote not configured"
        op_lock.release(OpKind.RESTORE)
        return False

    if not SNAPSHOT_RE.match(snapshot):
        restore_last_status = "error: invalid snapshot name"
        op_lock.release(OpKind.RESTORE)
        return False

    src = f"{remote_name}:{remote_path}/{snapshot}"
    logger.info("Starting restore from %s", src)

    try:
        proc = await asyncio.create_subprocess_exec(
            "rclone", "copy", src, str(ALL_APP_DATA),
            "--config", str(RCLONE_CONF),
            "--exclude", "backup/**", "-v",
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
        op_lock.release(OpKind.RESTORE)

    return restore_last_status == "success"


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
    if not RCLONE_CONF.exists():
        LOCAL_SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
        save_rclone_conf(f"[{DEFAULT_REMOTE_NAME}]\ntype = local\n")
        logger.info("Created default rclone.conf with local backup to %s", LOCAL_SNAPSHOTS_DIR)
    if not CONFIG_FILE.exists():
        save_config(dict(DEFAULT_CONFIG))
        logger.info("Created default config.json")


@app.before_serving
async def startup():
    global scheduler_task
    init_db()
    ensure_default_config()
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
    rclone_conf = load_rclone_conf()
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
    if op_lock.busy:
        return jsonify(ok=False, error=f"{op_lock.active.value} in progress"), 409
    data = await request.get_json()
    snapshot = data.get("snapshot", "")
    if not snapshot or not SNAPSHOT_RE.match(snapshot):
        return jsonify(ok=False, error="Invalid snapshot name"), 400
    asyncio.create_task(run_restore(snapshot))
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
    if op_lock.busy:
        return jsonify(ok=False, error=f"{op_lock.active.value} in progress"), 409
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
    subpath = request.args.get("path", "")
    if not validate_subpath(subpath):
        return jsonify(ok=False, error="Invalid path"), 400

    target = ALL_APP_DATA / subpath if subpath else ALL_APP_DATA
    if not target.exists() or not target.is_dir():
        return jsonify(ok=False, error="Directory not found"), 404

    try:
        target.resolve().relative_to(ALL_APP_DATA.resolve())
    except ValueError:
        return jsonify(ok=False, error="Invalid path"), 400

    files = []
    try:
        for entry in sorted(target.iterdir(), key=lambda e: (not e.is_dir(), e.name)):
            rel = entry.relative_to(ALL_APP_DATA)
            if str(rel).startswith("backup"):
                continue
            stat = entry.stat()
            files.append({
                "path": entry.name,
                "size": stat.st_size if entry.is_file() else 0,
                "is_dir": entry.is_dir(),
                "mod_time": datetime.fromtimestamp(
                    stat.st_mtime, tz=timezone.utc
                ).strftime("%Y-%m-%dT%H:%M:%S"),
            })
    except PermissionError:
        return jsonify(ok=False, error="Permission denied"), 403

    return jsonify(ok=True, files=files)


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
# Migration routes  (thin wrappers around the migration module)
# ---------------------------------------------------------------------------


@route("/api/migration/apps")
async def migration_apps():
    try:
        apps = await migration.get_apps_metadata(VM_DATA_DIR, ROUTER_URL)
        apps = [a for a in apps if a["name"] != "backup"]
        return jsonify(
            ok=True,
            apps=[
                {
                    "name": a["name"],
                    "repo_url": migration._strip_url_credentials(a.get("repo_url")),
                    "version": a.get("version"),
                    "status": a.get("status"),
                    "description": a.get("description"),
                }
                for a in apps
            ],
        )
    except Exception as e:
        logger.exception("Failed to list apps")
        return jsonify(ok=False, error=str(e)), 500


@route("/api/migration/export", methods=["POST"])
async def trigger_migration_export():
    err = op_lock.try_acquire(OpKind.MIGRATION)
    if err:
        return jsonify(ok=False, error=err), 409

    data = await request.get_json(silent=True) or {}
    app_filter = data.get("app")
    name = data.get("name")

    if name and not migration.validate_name(name):
        op_lock.release(OpKind.MIGRATION)
        return jsonify(ok=False, error="Invalid bundle name"), 400
    if app_filter and not migration.validate_name(app_filter):
        op_lock.release(OpKind.MIGRATION)
        return jsonify(ok=False, error="Invalid app name"), 400

    asyncio.create_task(migration.run_export(
        app_filter=app_filter,
        name=name,
        lock=op_lock,
        rclone_conf=RCLONE_CONF,
        all_app_data=ALL_APP_DATA,
        vm_data_dir=VM_DATA_DIR,
        router_url=ROUTER_URL,
        zone_domain=ZONE_DOMAIN,
        load_config=load_config,
    ))
    return jsonify(ok=True, message="Migration export started")


@route("/api/migration/status")
async def migration_status_endpoint():
    return jsonify(
        running=op_lock.migration_running,
        status=migration.status,
        log=migration.log[-50:],
    )


@route("/api/migration/bundles")
async def migration_bundles():
    bundles, remote_ok = await migration.list_bundles(RCLONE_CONF, load_config)
    result = []
    for b in bundles:
        entry = {"name": b}
        entry["type"] = "app" if "-app-" in b else "full"
        result.append(entry)
    return jsonify(ok=True, bundles=result, remote_ok=remote_ok)


@route("/api/migration/bundle/manifest")
async def bundle_manifest():
    name = request.args.get("bundle", "")
    if not name:
        return jsonify(ok=False, error="Missing bundle name"), 400
    if not migration.validate_name(name):
        return jsonify(ok=False, error="Invalid bundle name"), 400
    manifest = await migration.get_manifest(name, RCLONE_CONF, load_config)
    if manifest is None:
        return jsonify(ok=False, error="Could not load manifest"), 404
    return jsonify(ok=True, manifest=manifest)


@route("/api/migration/import", methods=["POST"])
async def trigger_migration_import():
    err = op_lock.try_acquire(OpKind.MIGRATION)
    if err:
        return jsonify(ok=False, error=err), 409

    data = await request.get_json(silent=True) or {}
    bundle = data.get("bundle")
    target_url = (data.get("target_url") or "").rstrip("/") or None
    target_token = data.get("target_token") or None
    selected_apps = data.get("apps")

    if selected_apps is not None and not isinstance(selected_apps, list):
        op_lock.release(OpKind.MIGRATION)
        return jsonify(ok=False, error="'apps' must be a list"), 400
    if not bundle:
        op_lock.release(OpKind.MIGRATION)
        return jsonify(ok=False, error="Missing bundle name"), 400
    if not migration.validate_name(bundle):
        op_lock.release(OpKind.MIGRATION)
        return jsonify(ok=False, error="Invalid bundle name"), 400

    if target_url and not target_token:
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            target_token = auth[7:]
    if target_url and not target_token:
        op_lock.release(OpKind.MIGRATION)
        return jsonify(ok=False, error="Missing target_token for remote import"), 400

    asyncio.create_task(migration.run_import(
        bundle_name=bundle,
        target_url=target_url,
        target_token=target_token,
        selected_apps=selected_apps,
        lock=op_lock,
        rclone_conf=RCLONE_CONF,
        all_app_data=ALL_APP_DATA,
        config_dir=CONFIG_DIR,
        load_config=load_config,
    ))
    return jsonify(ok=True, message="Migration import started")


@route("/api/migration/bundle/delete", methods=["POST"])
async def trigger_bundle_delete():
    data = await request.get_json(silent=True) or {}
    name = data.get("bundle", "")
    if not name:
        return jsonify(ok=False, error="Missing bundle name"), 400
    if not migration.validate_name(name):
        return jsonify(ok=False, error="Invalid bundle name"), 400
    ok = await migration.delete_bundle(name, RCLONE_CONF, load_config)
    return jsonify(ok=ok)


@route("/health")
async def health():
    return "ok"
