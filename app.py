print("backup app module loading...", flush=True)

import asyncio
import json
import logging
import os
import sqlite3
import subprocess
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
    """Initialize SQLite database and return a connection."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_FILE))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS backups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            status TEXT NOT NULL,
            error_message TEXT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        )
    """)
    conn.commit()
    return conn


def get_db():
    """Get a database connection (creates one per call, safe for sync use)."""
    return init_db()


def record_backup(timestamp, status, error_message=None):
    """Insert a backup record into the database."""
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO backups (timestamp, status, error_message) VALUES (?, ?, ?)",
            (timestamp, status, error_message),
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


# In-memory flag — not persisted (reset on startup is correct behavior)
backup_running = False

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


async def run_backup():
    global backup_running
    if backup_running:
        logger.warning("Backup already in progress, skipping")
        return False

    conf = load_config()
    remote_name = conf["remote_name"]
    remote_path = conf["remote_path"]

    if not RCLONE_CONF.exists():
        logger.error("No rclone.conf configured, skipping backup")
        record_backup("", "error", "no rclone.conf")
        return False

    if not remote_name or not remote_path:
        logger.error("Remote name or path not configured, skipping backup")
        record_backup("", "error", "remote not configured")
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
            record_backup(timestamp, "success")
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


@app.route("/")
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


@app.route("/api/config", methods=["GET"])
async def get_config():
    return jsonify(config=load_config(), rclone_conf=load_rclone_conf())


@app.route("/api/config", methods=["POST"])
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


@app.route("/api/setup-s3", methods=["POST"])
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


@app.route("/api/backup", methods=["POST"])
async def trigger_backup():
    if backup_running:
        return jsonify(ok=False, error="Backup already in progress"), 409
    asyncio.create_task(run_backup())
    return jsonify(ok=True, message="Backup started")


@app.route("/api/status")
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


@app.route("/health")
async def health():
    return "ok"
