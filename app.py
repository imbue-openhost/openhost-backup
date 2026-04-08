import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import sqlite3
import tempfile
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import httpx
from quart import Quart, render_template, request, jsonify

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
logger.info("backup app module loaded")

app = Quart(__name__)

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


def record_backup(
    timestamp, status, error_message=None, size_bytes=None, file_count=None, name=None
):
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

# Migration state
migration_running = False
migration_status = None  # dict with progress details
migration_log: list[str] = []

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
            "rclone",
            "copy",
            str(ALL_APP_DATA),
            dest,
            "--config",
            str(RCLONE_CONF),
            "--exclude",
            "backup/**",
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
            record_backup(
                timestamp,
                "success",
                size_bytes=size_bytes,
                file_count=file_count,
                name=name,
            )
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
            "rclone",
            "lsjson",
            f"{remote_name}:{remote_path}",
            "--config",
            str(RCLONE_CONF),
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
    except Exception:
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
        "rclone",
        "size",
        "--json",
        f"{remote_name}:{remote_path}/{snapshot}",
        "--config",
        str(RCLONE_CONF),
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
        "rclone",
        "lsjson",
        target,
        "--config",
        str(RCLONE_CONF),
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
                "rclone",
                "purge",
                f"{remote_name}:{remote_path}/{snapshot}",
                "--config",
                str(RCLONE_CONF),
                "--contimeout",
                "10s",
                "--timeout",
                "30s",
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
            "rclone",
            "copy",
            src,
            str(ALL_APP_DATA),
            "--config",
            str(RCLONE_CONF),
            "--exclude",
            "backup/**",
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


# ---------------------------------------------------------------------------
# Migration helpers
# ---------------------------------------------------------------------------

MIGRATION_BUNDLE_VERSION = 1
MIGRATION_DIR_PREFIX = "migration-"


def _migration_log(msg: str):
    """Append a timestamped message to the migration log."""
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    migration_log.append(entry)
    logger.info("migration: %s", msg)


async def _router_get(path: str, token: str | None = None, base_url: str | None = None):
    """GET request to the OpenHost router API."""
    url = (base_url or ROUTER_URL) + path
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    async with httpx.AsyncClient(verify=False, timeout=60) as client:
        r = await client.get(url, headers=headers)
        r.raise_for_status()
        return r.json()


async def _router_post(
    path: str,
    data: dict | None = None,
    token: str | None = None,
    base_url: str | None = None,
):
    """POST form-encoded request to the OpenHost router API."""
    url = (base_url or ROUTER_URL) + path
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    async with httpx.AsyncClient(verify=False, timeout=120) as client:
        r = await client.post(url, data=data or {}, headers=headers)
        r.raise_for_status()
        ct = r.headers.get("content-type", "")
        if "json" in ct:
            return r.json()
        return {"ok": True, "text": r.text}


async def _get_apps_metadata(token: str | None = None, base_url: str | None = None):
    """Query the router for full app details by reading the apps table directly.

    Returns list of dicts with app metadata needed for migration.
    If we have filesystem access to router.db, use that (more data).
    Otherwise fall back to the router API.
    """
    apps = []
    router_db = VM_DATA_DIR / "router.db"

    if router_db.exists():
        # Direct DB access — more complete data
        conn = sqlite3.connect(str(router_db))
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                "SELECT name, manifest_name, version, description, repo_url, "
                "health_check, local_port, container_port, status, memory_mb, "
                "cpu_millicores, gpu, public_paths, manifest_raw, runtime_type "
                "FROM apps ORDER BY name"
            ).fetchall()
            for row in rows:
                apps.append(
                    {
                        "name": row["name"],
                        "manifest_name": row["manifest_name"],
                        "version": row["version"],
                        "description": row["description"],
                        "repo_url": row["repo_url"],
                        "health_check": row["health_check"],
                        "status": row["status"],
                        "memory_mb": row["memory_mb"],
                        "cpu_millicores": row["cpu_millicores"],
                        "gpu": row["gpu"],
                        "public_paths": row["public_paths"],
                        "manifest_raw": row["manifest_raw"],
                        "runtime_type": row["runtime_type"],
                    }
                )
        finally:
            conn.close()
    else:
        # Fall back to API
        data = await _router_get("/api/apps", token=token, base_url=base_url)
        for name, info in data.items():
            apps.append(
                {
                    "name": name,
                    "status": info.get("status"),
                    "repo_url": None,
                    "manifest_raw": None,
                }
            )

    return apps


def _safe_copy_sqlite(src: Path, dst: Path):
    """Copy a SQLite database safely using the backup API."""
    src_conn = sqlite3.connect(str(src))
    dst_conn = sqlite3.connect(str(dst))
    try:
        src_conn.backup(dst_conn)
    finally:
        dst_conn.close()
        src_conn.close()


async def _build_manifest(apps: list[dict], app_filter: str | None = None) -> dict:
    """Build a migration manifest.json."""
    if app_filter:
        apps = [a for a in apps if a["name"] == app_filter]

    return {
        "version": MIGRATION_BUNDLE_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_instance": ZONE_DOMAIN or "unknown",
        "apps": [
            {
                "name": a["name"],
                "repo_url": a.get("repo_url"),
                "version": a.get("version"),
                "description": a.get("description"),
                "manifest_raw": a.get("manifest_raw"),
                "memory_mb": a.get("memory_mb"),
                "cpu_millicores": a.get("cpu_millicores"),
                "runtime_type": a.get("runtime_type"),
            }
            for a in apps
        ],
    }


async def run_migration_export(app_filter: str | None = None, name: str | None = None):
    """Export a migration bundle to the configured rclone remote.

    If app_filter is set, only export that single app.
    """
    global migration_running, migration_status
    if migration_running:
        _migration_log("Migration already in progress")
        return False

    migration_running = True
    migration_log.clear()
    migration_status = {"phase": "starting", "progress": 0}

    conf = load_config()
    remote_name = conf["remote_name"]
    remote_path = conf["remote_path"]

    try:
        if not RCLONE_CONF.exists():
            raise RuntimeError("No rclone.conf configured")

        # 1. Gather app metadata
        _migration_log("Gathering app metadata from router...")
        migration_status = {"phase": "gathering_metadata", "progress": 5}
        apps = await _get_apps_metadata()

        if app_filter:
            matching = [a for a in apps if a["name"] == app_filter]
            if not matching:
                raise RuntimeError(f"App '{app_filter}' not found")
            # Don't include the backup app itself in single-app exports
        else:
            # Exclude backup app from full instance exports
            apps = [a for a in apps if a["name"] != "backup"]

        manifest = await _build_manifest(apps, app_filter)
        app_names = [a["name"] for a in manifest["apps"]]
        _migration_log(f"Found {len(app_names)} apps: {', '.join(app_names)}")

        # 2. Create a temp staging directory
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        bundle_name = f"{MIGRATION_DIR_PREFIX}{timestamp}"
        if app_filter:
            bundle_name = f"{MIGRATION_DIR_PREFIX}app-{app_filter}-{timestamp}"
        if name:
            bundle_name = f"{MIGRATION_DIR_PREFIX}{name}-{timestamp}"

        with tempfile.TemporaryDirectory() as tmpdir:
            staging = Path(tmpdir) / bundle_name
            staging.mkdir()

            # 3. Write manifest
            manifest_path = staging / "manifest.json"
            manifest_path.write_text(json.dumps(manifest, indent=2))
            _migration_log("Wrote manifest.json")
            migration_status = {"phase": "copying_metadata", "progress": 10}

            # 4. Copy router.db safely (full instance export only)
            if not app_filter:
                router_db = VM_DATA_DIR / "router.db"
                if router_db.exists():
                    vm_staging = staging / "vm_data"
                    vm_staging.mkdir()
                    _safe_copy_sqlite(router_db, vm_staging / "router.db")
                    _migration_log("Copied router.db (safe backup)")

                    # Copy identity keys
                    keys_dir = VM_DATA_DIR / "identity_keys"
                    if keys_dir.exists():
                        keys_staging = vm_staging / "identity_keys"
                        keys_staging.mkdir()
                        for key_file in keys_dir.iterdir():
                            if key_file.is_file():
                                shutil.copy2(key_file, keys_staging / key_file.name)
                        _migration_log("Copied identity keys")

            migration_status = {"phase": "copying_app_data", "progress": 20}

            # 5. Copy app data for selected apps
            app_data_staging = staging / "app_data"
            app_data_staging.mkdir()

            for i, app_name in enumerate(app_names):
                src = ALL_APP_DATA / app_name
                if src.exists():
                    _migration_log(f"Copying data for app: {app_name}")
                    # Use rclone for the actual copy to handle large dirs efficiently
                    dst = app_data_staging / app_name
                    proc = await asyncio.create_subprocess_exec(
                        "rclone",
                        "copy",
                        str(src),
                        str(dst),
                        "--config",
                        str(RCLONE_CONF),
                        "-v",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.STDOUT,
                    )
                    stdout, _ = await proc.communicate()
                    if proc.returncode != 0:
                        _migration_log(f"Warning: failed to copy data for {app_name}")
                else:
                    _migration_log(f"No data directory for app: {app_name}")

                pct = 20 + int(60 * (i + 1) / len(app_names))
                migration_status = {
                    "phase": "copying_app_data",
                    "progress": pct,
                    "current_app": app_name,
                }

            # 6. Upload the staged bundle to the remote
            _migration_log("Uploading migration bundle to remote storage...")
            migration_status = {"phase": "uploading", "progress": 85}

            dest = f"{remote_name}:{remote_path}/migrations/{bundle_name}"
            proc = await asyncio.create_subprocess_exec(
                "rclone",
                "copy",
                str(staging),
                dest,
                "--config",
                str(RCLONE_CONF),
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

            _migration_log(f"Migration bundle uploaded: {bundle_name}")
            migration_status = {"phase": "done", "progress": 100, "bundle": bundle_name}
            return True

    except Exception as e:
        _migration_log(f"Export failed: {e}")
        migration_status = {"phase": "error", "progress": 0, "error": str(e)}
        return False
    finally:
        migration_running = False


async def list_migration_bundles():
    """List available migration bundles on the remote."""
    conf = load_config()
    remote_name = conf["remote_name"]
    remote_path = conf["remote_path"]

    if not RCLONE_CONF.exists() or not remote_name or not remote_path:
        return [], False

    try:
        proc = await asyncio.create_subprocess_exec(
            "rclone",
            "lsjson",
            f"{remote_name}:{remote_path}/migrations",
            "--config",
            str(RCLONE_CONF),
            "--dirs-only",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            err = stderr.decode().strip()
            if "directory not found" in err.lower() or "not found" in err.lower():
                return [], True  # no migrations dir yet is fine
            logger.error("Failed to list migration bundles: %s", err)
            return [], False

        entries = json.loads(stdout.decode())
        names = sorted(
            [
                e["Path"]
                for e in entries
                if e.get("IsDir") and e["Path"].startswith(MIGRATION_DIR_PREFIX)
            ],
            reverse=True,
        )
        return names, True
    except Exception:
        logger.exception("Failed to list migration bundles")
        return [], False


async def get_migration_manifest(bundle_name: str) -> dict | None:
    """Download and parse the manifest.json from a migration bundle."""
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
            str(RCLONE_CONF),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc.communicate()
        if proc.returncode != 0 or not dst.exists():
            return None
        return json.loads(dst.read_text())


async def run_migration_import(
    bundle_name: str,
    target_url: str,
    target_token: str,
    selected_apps: list[str] | None = None,
):
    """Import a migration bundle onto a target instance.

    Args:
        bundle_name: Name of the migration bundle on the remote.
        target_url: URL of the target OpenHost instance (e.g. https://my-instance.selfhost.imbue.com)
        target_token: API bearer token for the target instance.
        selected_apps: Optional list of app names to import. If None, import all.
    """
    global migration_running, migration_status
    if migration_running:
        _migration_log("Migration already in progress")
        return False

    migration_running = True
    migration_log.clear()
    migration_status = {"phase": "starting", "progress": 0}

    conf = load_config()
    remote_name = conf["remote_name"]
    remote_path = conf["remote_path"]

    try:
        # 1. Download the manifest
        _migration_log(f"Downloading manifest from bundle: {bundle_name}")
        migration_status = {"phase": "downloading_manifest", "progress": 5}

        manifest = await get_migration_manifest(bundle_name)
        if not manifest:
            raise RuntimeError("Could not download or parse manifest.json")

        apps_in_bundle = manifest.get("apps", [])
        if selected_apps:
            apps_in_bundle = [a for a in apps_in_bundle if a["name"] in selected_apps]

        if not apps_in_bundle:
            raise RuntimeError("No apps found in bundle (or none match selection)")

        app_names = [a["name"] for a in apps_in_bundle]
        _migration_log(f"Bundle from: {manifest.get('source_instance', 'unknown')}")
        _migration_log(f"Apps to import: {', '.join(app_names)}")

        # 2. Check which apps already exist on the target
        _migration_log("Checking existing apps on target instance...")
        migration_status = {"phase": "checking_target", "progress": 10}

        try:
            existing = await _router_get(
                "/api/apps", token=target_token, base_url=target_url
            )
        except Exception as e:
            raise RuntimeError(f"Cannot reach target instance: {e}")

        existing_names = set(existing.keys()) if isinstance(existing, dict) else set()

        # 3. Deploy apps that don't exist yet on the target
        migration_status = {"phase": "deploying_apps", "progress": 15}
        apps_to_deploy = [
            a
            for a in apps_in_bundle
            if a["name"] not in existing_names and a.get("repo_url")
        ]
        apps_existing = [a for a in apps_in_bundle if a["name"] in existing_names]

        if apps_to_deploy:
            _migration_log(f"Deploying {len(apps_to_deploy)} new apps on target...")
            for i, app_info in enumerate(apps_to_deploy):
                app_name = app_info["name"]
                repo_url = app_info.get("repo_url")
                if not repo_url:
                    _migration_log(f"Skipping {app_name}: no repo_url in manifest")
                    continue

                _migration_log(f"Deploying {app_name} from {repo_url}...")
                try:
                    await _router_post(
                        "/api/add_app",
                        data={"repo_url": repo_url, "app_name": app_name},
                        token=target_token,
                        base_url=target_url,
                    )
                    _migration_log(f"Deployed {app_name} (building...)")
                except Exception as e:
                    _migration_log(f"Failed to deploy {app_name}: {e}")

                pct = 15 + int(20 * (i + 1) / len(apps_to_deploy))
                migration_status = {
                    "phase": "deploying_apps",
                    "progress": pct,
                    "current_app": app_name,
                }

        # 4. Wait for deployed apps to be ready
        if apps_to_deploy:
            _migration_log("Waiting for newly deployed apps to build and start...")
            migration_status = {"phase": "waiting_for_apps", "progress": 40}
            max_wait = 300  # 5 minutes
            waited = 0
            while waited < max_wait:
                await asyncio.sleep(10)
                waited += 10
                try:
                    current = await _router_get(
                        "/api/apps", token=target_token, base_url=target_url
                    )
                    all_ready = True
                    for a in apps_to_deploy:
                        status = current.get(a["name"], {}).get("status", "")
                        if status in ("building", "starting"):
                            all_ready = False
                            break
                    if all_ready:
                        break
                except Exception:
                    pass
            _migration_log(f"Apps ready after ~{waited}s")

        # 5. Stop all target apps that we're importing data for
        _migration_log("Stopping apps on target before data restore...")
        migration_status = {"phase": "stopping_apps", "progress": 45}

        for app_name in app_names:
            try:
                await _router_post(
                    f"/stop_app/{app_name}", token=target_token, base_url=target_url
                )
                _migration_log(f"Stopped {app_name}")
            except Exception as e:
                _migration_log(f"Could not stop {app_name}: {e}")

        # Give containers a moment to fully stop
        await asyncio.sleep(3)

        # 6. Download app data from bundle and push to target's app_data
        _migration_log("Restoring app data from migration bundle...")
        migration_status = {"phase": "restoring_data", "progress": 50}

        bundle_base = f"{remote_name}:{remote_path}/migrations/{bundle_name}"

        for i, app_name in enumerate(app_names):
            src = f"{bundle_base}/app_data/{app_name}"
            dst = str(ALL_APP_DATA / app_name)
            _migration_log(f"Restoring data for: {app_name}")

            proc = await asyncio.create_subprocess_exec(
                "rclone",
                "copy",
                src,
                dst,
                "--config",
                str(RCLONE_CONF),
                "-v",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            stdout, _ = await proc.communicate()
            if proc.returncode != 0:
                _migration_log(
                    f"Warning: data restore may have partial errors for {app_name}"
                )
            else:
                _migration_log(f"Data restored for {app_name}")

            pct = 50 + int(35 * (i + 1) / len(app_names))
            migration_status = {
                "phase": "restoring_data",
                "progress": pct,
                "current_app": app_name,
            }

        # 7. Restart all apps on target
        _migration_log("Restarting apps on target...")
        migration_status = {"phase": "restarting_apps", "progress": 90}

        for app_name in app_names:
            try:
                await _router_post(
                    f"/reload_app/{app_name}", token=target_token, base_url=target_url
                )
                _migration_log(f"Restarted {app_name}")
            except Exception as e:
                _migration_log(f"Could not restart {app_name}: {e}")

        # 8. Verify apps are running
        _migration_log("Verifying apps on target...")
        migration_status = {"phase": "verifying", "progress": 95}
        await asyncio.sleep(10)

        try:
            final_status = await _router_get(
                "/api/apps", token=target_token, base_url=target_url
            )
            for app_name in app_names:
                status = final_status.get(app_name, {}).get("status", "unknown")
                error = final_status.get(app_name, {}).get("error_message", "")
                if status == "running":
                    _migration_log(f"{app_name}: running")
                else:
                    _migration_log(
                        f"{app_name}: {status}" + (f" ({error})" if error else "")
                    )
        except Exception as e:
            _migration_log(f"Could not verify final status: {e}")

        _migration_log("Migration import complete!")
        migration_status = {"phase": "done", "progress": 100, "bundle": bundle_name}
        return True

    except Exception as e:
        _migration_log(f"Import failed: {e}")
        migration_status = {"phase": "error", "progress": 0, "error": str(e)}
        return False
    finally:
        migration_running = False


async def delete_migration_bundle(bundle_name: str) -> bool:
    """Delete a migration bundle from remote storage."""
    conf = load_config()
    remote_name = conf["remote_name"]
    remote_path = conf["remote_path"]

    if not RCLONE_CONF.exists():
        return False

    try:
        proc = await asyncio.create_subprocess_exec(
            "rclone",
            "purge",
            f"{remote_name}:{remote_path}/migrations/{bundle_name}",
            "--config",
            str(RCLONE_CONF),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
        if proc.returncode == 0:
            _migration_log(f"Deleted migration bundle: {bundle_name}")
            return True
        else:
            err = stderr.decode().strip()
            _migration_log(f"Failed to delete bundle: {err}")
            return False
    except Exception as e:
        _migration_log(f"Delete failed: {e}")
        return False


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
    """Set up local backup config if nothing is configured yet."""
    if not RCLONE_CONF.exists():
        LOCAL_SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
        save_rclone_conf(f"[{DEFAULT_REMOTE_NAME}]\ntype = local\n")
        logger.info(
            "Created default rclone.conf with local backup to %s", LOCAL_SNAPSHOTS_DIR
        )
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
            files.append(
                {
                    "path": entry.name,
                    "size": stat.st_size if entry.is_file() else 0,
                    "is_dir": entry.is_dir(),
                    "mod_time": datetime.fromtimestamp(
                        stat.st_mtime, tz=timezone.utc
                    ).strftime("%Y-%m-%dT%H:%M:%S"),
                }
            )
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


# ---------------------------------------------------------------------------
# Migration API routes
# ---------------------------------------------------------------------------


@route("/api/migration/apps")
async def migration_apps():
    """List apps available for migration export."""
    try:
        apps = await _get_apps_metadata()
        # Exclude the backup app itself
        apps = [a for a in apps if a["name"] != "backup"]
        return jsonify(
            ok=True,
            apps=[
                {
                    "name": a["name"],
                    "repo_url": a.get("repo_url"),
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
    """Start a migration export."""
    if migration_running:
        return jsonify(ok=False, error="Migration already in progress"), 409
    if backup_running:
        return jsonify(ok=False, error="Backup in progress"), 409

    data = await request.get_json(silent=True) or {}
    app_filter = data.get("app")  # optional: single app name
    name = data.get("name")  # optional: custom bundle label

    asyncio.create_task(run_migration_export(app_filter=app_filter, name=name))
    return jsonify(ok=True, message="Migration export started")


@route("/api/migration/status")
async def migration_status_endpoint():
    """Poll migration progress."""
    return jsonify(
        running=migration_running,
        status=migration_status,
        log=migration_log[-50:],  # last 50 lines
    )


@route("/api/migration/bundles")
async def migration_bundles():
    """List available migration bundles."""
    bundles, remote_ok = await list_migration_bundles()
    result = []
    for b in bundles:
        entry = {"name": b}
        # Try to classify
        if "-app-" in b:
            entry["type"] = "app"
        else:
            entry["type"] = "full"
        result.append(entry)
    return jsonify(ok=True, bundles=result, remote_ok=remote_ok)


@route("/api/migration/bundle/manifest")
async def bundle_manifest():
    """Get the manifest of a specific migration bundle."""
    name = request.args.get("bundle", "")
    if not name:
        return jsonify(ok=False, error="Missing bundle name"), 400
    manifest = await get_migration_manifest(name)
    if manifest is None:
        return jsonify(ok=False, error="Could not load manifest"), 404
    return jsonify(ok=True, manifest=manifest)


@route("/api/migration/import", methods=["POST"])
async def trigger_migration_import():
    """Start a migration import."""
    if migration_running:
        return jsonify(ok=False, error="Migration already in progress"), 409

    data = await request.get_json(silent=True) or {}
    bundle = data.get("bundle")
    target_url = data.get("target_url", "").rstrip("/")
    target_token = data.get("target_token", "")
    selected_apps = data.get("apps")  # optional list of app names

    if not bundle:
        return jsonify(ok=False, error="Missing bundle name"), 400

    # If importing to the same instance, use local router URL
    if not target_url:
        target_url = ROUTER_URL
    if not target_token:
        # Try to use the same token used for this request
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            target_token = auth[7:]

    if not target_token:
        return jsonify(ok=False, error="Missing target_token"), 400

    asyncio.create_task(
        run_migration_import(bundle, target_url, target_token, selected_apps)
    )
    return jsonify(ok=True, message="Migration import started")


@route("/api/migration/bundle/delete", methods=["POST"])
async def trigger_bundle_delete():
    """Delete a migration bundle from remote storage."""
    data = await request.get_json(silent=True) or {}
    name = data.get("bundle", "")
    if not name:
        return jsonify(ok=False, error="Missing bundle name"), 400
    ok = await delete_migration_bundle(name)
    return jsonify(ok=ok)


@route("/health")
async def health():
    return "ok"
