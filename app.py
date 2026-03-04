import asyncio
import json
import logging
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from quart import Quart, render_template, request, jsonify

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Quart(__name__)

BASE_PATH = os.environ.get("OPENHOST_APP_BASE_PATH", "/backup")
APP_DATA_DIR = Path(os.environ.get("OPENHOST_APP_DATA_DIR", "/data/app_data/backup"))
ALL_APP_DATA = Path("/data/app_data")

CONFIG_DIR = APP_DATA_DIR
RCLONE_CONF = CONFIG_DIR / "rclone.conf"
CONFIG_FILE = CONFIG_DIR / "config.json"

DEFAULT_CONFIG = {
    "interval_seconds": 3600,
    "remote_name": "openhost-backup",
    "remote_path": "",
}

# Backup state
backup_state = {
    "last_backup": None,
    "last_status": None,
    "running": False,
}

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
    if backup_state["running"]:
        logger.warning("Backup already in progress, skipping")
        return False

    conf = load_config()
    remote_name = conf["remote_name"]
    remote_path = conf["remote_path"]

    if not RCLONE_CONF.exists():
        logger.error("No rclone.conf configured, skipping backup")
        backup_state["last_status"] = "error: no rclone.conf"
        return False

    if not remote_name or not remote_path:
        logger.error("Remote name or path not configured, skipping backup")
        backup_state["last_status"] = "error: remote not configured"
        return False

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    dest = f"{remote_name}:{remote_path}/{timestamp}"

    backup_state["running"] = True
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
            backup_state["last_backup"] = timestamp
            backup_state["last_status"] = "success"
            logger.info("Backup completed successfully")
        else:
            backup_state["last_status"] = f"error: rclone exit code {proc.returncode}"
            logger.error("Backup failed with exit code %d", proc.returncode)
    except Exception as e:
        backup_state["last_status"] = f"error: {e}"
        logger.exception("Backup failed")
    finally:
        backup_state["running"] = False

    return backup_state["last_status"] == "success"


async def scheduler_loop():
    while True:
        conf = load_config()
        interval = conf["interval_seconds"]
        logger.info("Next backup in %d seconds", interval)
        await asyncio.sleep(interval)
        await run_backup()


@app.before_serving
async def startup():
    global scheduler_task
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
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
    return await render_template(
        "index.html",
        base_path=BASE_PATH,
        config=conf,
        rclone_conf=rclone_conf,
        state=backup_state,
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
    if backup_state["running"]:
        return jsonify(ok=False, error="Backup already in progress"), 409
    asyncio.create_task(run_backup())
    return jsonify(ok=True, message="Backup started")


@app.route("/api/status")
async def status():
    conf = load_config()
    return jsonify(
        running=backup_state["running"],
        last_backup=backup_state["last_backup"],
        last_status=backup_state["last_status"],
        interval_seconds=conf["interval_seconds"],
    )


@app.route("/health")
async def health():
    return "ok"
