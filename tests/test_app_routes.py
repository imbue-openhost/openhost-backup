"""Integration tests for app.py HTTP routes related to migration.

Tests the Quart routes for the migration receive endpoints using the
Quart test client.
"""

from __future__ import annotations

import io
import json
import os
import tarfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

# Set required env vars before importing app
os.environ.setdefault("OPENHOST_APP_DATA_DIR", "/tmp/test_backup_data")
os.environ.setdefault("OPENHOST_APP_BASE_PATH", "/backup")

import app as backup_app


@pytest.fixture
def client(tmp_path):
    """Create a Quart test client with isolated data directories."""
    # Override paths so tests don't touch real data
    backup_app.ALL_APP_DATA = tmp_path / "app_data"
    backup_app.ALL_APP_DATA.mkdir()
    backup_app.APP_DATA_DIR = tmp_path / "backup_data"
    backup_app.APP_DATA_DIR.mkdir()
    backup_app.CONFIG_DIR = backup_app.APP_DATA_DIR
    backup_app.DB_FILE = backup_app.APP_DATA_DIR / "backups.db"
    backup_app.CONFIG_FILE = backup_app.APP_DATA_DIR / "config.json"
    backup_app.RESTIC_REPO_DIR = backup_app.APP_DATA_DIR / "restic-repo"

    # Init DB
    backup_app.init_db()

    return backup_app.app.test_client()


def _make_tar_gz(contents: dict[str, bytes]) -> bytes:
    """Create a tar.gz in memory with the given path->content mapping."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, data in contents.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


class TestReceiveDataEndpoint:
    """Tests for POST /api/migration/receive/data."""

    async def test_empty_body_returns_400(self, client):
        response = await client.post(
            "/api/migration/receive/data",
            data=b"",
            headers={"Content-Type": "application/gzip"},
        )
        assert response.status_code == 400
        data = await response.get_json()
        assert data["ok"] is False
        assert "Empty" in data["error"]

    async def test_valid_tar_extracts_apps(self, client, tmp_path):
        tar_data = _make_tar_gz(
            {
                "myapp/config.json": b'{"key": "value"}',
                "myapp/data.db": b"database content",
                "secrets/sqlite/main.db": b"secret data",
            }
        )
        response = await client.post(
            "/api/migration/receive/data",
            data=tar_data,
            headers={"Content-Type": "application/gzip"},
        )
        assert response.status_code == 200
        data = await response.get_json()
        assert data["ok"] is True
        assert "myapp" in data.get("apps", [])
        assert "secrets" in data.get("apps", [])

        # Verify files were extracted
        assert (backup_app.ALL_APP_DATA / "myapp" / "config.json").exists()
        assert (backup_app.ALL_APP_DATA / "secrets" / "sqlite" / "main.db").exists()

    async def test_corrupt_tar_returns_error(self, client):
        response = await client.post(
            "/api/migration/receive/data",
            data=b"not a tar file at all",
            headers={"Content-Type": "application/gzip"},
        )
        assert response.status_code == 400
        data = await response.get_json()
        assert data["ok"] is False


class TestReceiveStartEndpoint:
    """Tests for POST /api/migration/receive/start."""

    async def test_missing_manifest_returns_400(self, client):
        # Reset op_lock
        backup_app.op_lock._active = None

        response = await client.post(
            "/api/migration/receive/start",
            data=json.dumps({}),
            headers={"Content-Type": "application/json"},
        )
        # Empty manifest -> receive_start returns error -> 400
        data = await response.get_json()
        assert data["ok"] is False

    async def test_valid_manifest_accepted(self, client):
        backup_app.op_lock._active = None

        manifest = {
            "version": 3,
            "apps": [{"name": "testapp"}],
            "source_instance": "test.example.com",
        }
        response = await client.post(
            "/api/migration/receive/start",
            data=json.dumps(manifest),
            headers={"Content-Type": "application/json"},
        )
        assert response.status_code == 200
        data = await response.get_json()
        assert data["ok"] is True
        assert "testapp" in data["accepted_apps"]

        # Clean up lock
        if backup_app.op_lock.active:
            backup_app.op_lock.release(backup_app.op_lock.active)

    async def test_lock_conflict_returns_409(self, client):
        from operations import OpKind

        backup_app.op_lock._active = OpKind.BACKUP

        manifest = {"apps": [{"name": "testapp"}]}
        response = await client.post(
            "/api/migration/receive/start",
            data=json.dumps(manifest),
            headers={"Content-Type": "application/json"},
        )
        assert response.status_code == 409

        # Clean up
        backup_app.op_lock._active = None


class TestReceiveFinalizeEndpoint:
    """Tests for POST /api/migration/receive/finalize."""

    async def test_missing_manifest_returns_400(self, client):
        backup_app.op_lock._active = None

        response = await client.post(
            "/api/migration/receive/finalize",
            data=json.dumps({}),
            headers={"Content-Type": "application/json"},
        )
        data = await response.get_json()
        assert data["ok"] is False
        assert "Missing" in data.get("error", "")

    @patch("migration._router_post")
    async def test_finalize_with_manifest(self, mock_post, client):
        from operations import OpKind

        backup_app.op_lock._active = OpKind.MIGRATION
        mock_post.return_value = {"ok": True}

        manifest = {
            "apps": [{"name": "testapp", "status": "running"}],
        }
        response = await client.post(
            "/api/migration/receive/finalize",
            data=json.dumps({"manifest": manifest}),
            headers={"Content-Type": "application/json"},
        )
        data = await response.get_json()
        assert data["ok"] is True

        # Lock should be released after finalize
        assert backup_app.op_lock.active is None


class TestAppsStatusEndpoint:
    """Tests for GET /api/apps-status."""

    @patch("app._get_router_apps")
    async def test_returns_apps(self, mock_get, client):
        mock_get.return_value = {
            "secrets": {"status": "running"},
            "backup": {"status": "running"},
        }
        # Need a router token
        backup_app.ROUTER_API_TOKEN = "test-token"
        response = await client.get("/api/apps-status")
        data = await response.get_json()
        assert data["ok"] is True
        assert "secrets" in data["apps"]
        backup_app.ROUTER_API_TOKEN = ""

    async def test_no_token_returns_400(self, client):
        backup_app.ROUTER_API_TOKEN = ""
        response = await client.get("/api/apps-status")
        assert response.status_code == 400


class TestStopAllAppsEndpoint:
    """Tests for POST /api/stop-all-apps."""

    @patch("app._get_router_apps")
    async def test_stops_running_apps(self, mock_get, client):
        mock_get.return_value = {
            "secrets": {"status": "running"},
            "backup": {"status": "running"},
            "agent": {"status": "stopped"},
        }

        # Mock the httpx module used inside the function
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response

        backup_app.ROUTER_API_TOKEN = "test-token"
        with patch("httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
            response = await client.post("/api/stop-all-apps")
        data = await response.get_json()
        assert data["ok"] is True
        assert "secrets" in data["stopped"]
        assert "backup" not in data["stopped"]  # backup is never stopped
        assert "agent" not in data["stopped"]  # already stopped
        backup_app.ROUTER_API_TOKEN = ""


class TestChownAppDataEndpoint:
    """Tests for POST /api/chown-app-data."""

    @patch("os.chown")
    @patch("app._get_router_apps")
    async def test_chown_when_all_stopped(self, mock_get, mock_chown, client):
        mock_get.return_value = {
            "secrets": {"status": "stopped"},
            "backup": {"status": "running"},
        }
        # Create a test file so os.walk has something to iterate
        (backup_app.ALL_APP_DATA / "testapp").mkdir(exist_ok=True)
        (backup_app.ALL_APP_DATA / "testapp" / "data.db").touch()

        backup_app.ROUTER_API_TOKEN = "test-token"
        response = await client.post("/api/chown-app-data")
        data = await response.get_json()
        assert data["ok"] is True
        assert data["count"] > 0
        # Verify chown was called with uid=1000, gid=1000
        for call_args in mock_chown.call_args_list:
            assert call_args[0][1] == 1000  # uid
            assert call_args[0][2] == 1000  # gid
        backup_app.ROUTER_API_TOKEN = ""

    @patch("app._get_router_apps")
    async def test_chown_rejected_when_apps_running(self, mock_get, client):
        mock_get.return_value = {
            "secrets": {"status": "running"},
            "backup": {"status": "running"},
        }
        backup_app.ROUTER_API_TOKEN = "test-token"
        response = await client.post("/api/chown-app-data")
        assert response.status_code == 400
        data = await response.get_json()
        assert "still running" in data["error"]
        backup_app.ROUTER_API_TOKEN = ""

    async def test_chown_no_token_returns_400(self, client):
        backup_app.ROUTER_API_TOKEN = ""
        response = await client.post("/api/chown-app-data")
        assert response.status_code == 400


class TestHealthEndpoint:
    async def test_health(self, client):
        response = await client.get("/health")
        assert response.status_code == 200


class TestClassifyRepo:
    def test_local_path(self):
        assert backup_app.classify_repo("/var/backups/restic") == {
            "type": "local",
            "remote": False,
            "location": "/var/backups/restic",
        }

    def test_local_prefix(self):
        assert backup_app.classify_repo("local:/var/backups") == {
            "type": "local",
            "remote": False,
            "location": "/var/backups",
        }

    def test_s3(self):
        r = backup_app.classify_repo("s3:s3.us-east-1.amazonaws.com/mybucket/path")
        assert r["type"] == "s3"
        assert r["remote"] is True
        assert r["location"] == "s3.us-east-1.amazonaws.com/mybucket/path"

    def test_b2(self):
        r = backup_app.classify_repo("b2:bucket:path")
        assert r["type"] == "b2"
        assert r["remote"] is True

    def test_sftp(self):
        r = backup_app.classify_repo("sftp:user@host:/data")
        assert r["type"] == "sftp"
        assert r["remote"] is True

    def test_empty(self):
        assert backup_app.classify_repo("") == {
            "type": "unknown",
            "remote": False,
            "location": "",
        }


class TestConfigEnv:
    async def test_env_set_and_redacted(self, client):
        # Start with the default config in the fixture's CONFIG_FILE.
        backup_app.ensure_default_config()

        # Set a whitelisted env var.
        resp = await client.post(
            "/api/config",
            data=json.dumps({"env": {"AWS_ACCESS_KEY_ID": "test-key"}}),
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 200
        data = await resp.get_json()
        assert data["ok"] is True

        # GET should redact the value but list the key.
        resp2 = await client.get("/api/config")
        data2 = await resp2.get_json()
        assert data2["config"]["env"]["AWS_ACCESS_KEY_ID"] == "***"
        assert "AWS_ACCESS_KEY_ID" in data2["config"]["env_keys"]

        # Underlying config actually stores the raw value.
        conf = backup_app.load_config()
        assert conf["env"]["AWS_ACCESS_KEY_ID"] == "test-key"

    async def test_env_disallowed_key_rejected(self, client):
        resp = await client.post(
            "/api/config",
            data=json.dumps({"env": {"PATH": "/evil"}}),
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 400
        data = await resp.get_json()
        assert "not allowed" in data["error"]

    async def test_env_clear(self, client):
        backup_app.ensure_default_config()
        # Seed a value.
        conf = backup_app.load_config()
        conf["env"] = {"AWS_ACCESS_KEY_ID": "to-be-cleared"}
        backup_app.save_config(conf)

        # Clear it by passing empty string.
        resp = await client.post(
            "/api/config",
            data=json.dumps({"env": {"AWS_ACCESS_KEY_ID": ""}}),
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 200
        conf = backup_app.load_config()
        assert "AWS_ACCESS_KEY_ID" not in (conf.get("env") or {})


class TestStatusBackend:
    async def test_status_includes_backend(self, client):
        backup_app.ensure_default_config()
        resp = await client.get("/api/status")
        data = await resp.get_json()
        assert "backend" in data
        # Default is a local path.
        assert data["backend"]["type"] == "local"
        assert data["backend"]["remote"] is False


class TestPasswordReveal:
    async def test_password_reveal_requires_auth(self, client):
        backup_app.ensure_default_config()
        resp = await client.get("/api/config/password")
        assert resp.status_code == 401
        data = await resp.get_json()
        assert data["ok"] is False

    @patch("app._verify_admin_token", new_callable=AsyncMock)
    async def test_password_reveal_with_valid_token(self, mock_verify, client):
        backup_app.ensure_default_config()
        mock_verify.return_value = True
        resp = await client.get(
            "/api/config/password",
            headers={"Authorization": "Bearer valid-token"},
        )
        assert resp.status_code == 200
        data = await resp.get_json()
        assert data["ok"] is True
        # Password is the auto-generated one from ensure_default_config.
        assert data["password"]

    @patch("app._verify_admin_token", new_callable=AsyncMock)
    async def test_password_reveal_with_invalid_token(self, mock_verify, client):
        mock_verify.return_value = False
        resp = await client.get(
            "/api/config/password",
            headers={"Authorization": "Bearer bad-token"},
        )
        assert resp.status_code == 401


class TestConfigRedactsRouterToken:
    async def test_router_api_token_redacted(self, client):
        # Seed a token directly.
        backup_app.ensure_default_config()
        conf = backup_app.load_config()
        conf["router_api_token"] = "secret-router-token"
        backup_app.save_config(conf)

        resp = await client.get("/api/config")
        data = await resp.get_json()
        assert data["config"]["router_api_token"] == "***"
        # Underlying storage still has the real value.
        assert backup_app.load_config()["router_api_token"] == "secret-router-token"


class TestPostConfigSensitiveWrites:
    async def test_first_router_token_bootstraps_without_auth(self, client):
        # Fresh install — no token yet — setting one should succeed.
        backup_app.ensure_default_config()
        # Make sure it's really empty.
        conf = backup_app.load_config()
        conf["router_api_token"] = ""
        backup_app.save_config(conf)

        resp = await client.post(
            "/api/config",
            data=json.dumps({"router_api_token": "first-token"}),
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 200

    async def test_rotate_router_token_requires_auth(self, client):
        backup_app.ensure_default_config()
        conf = backup_app.load_config()
        conf["router_api_token"] = "existing"
        backup_app.save_config(conf)

        resp = await client.post(
            "/api/config",
            data=json.dumps({"router_api_token": "rotated"}),
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 401
        # Token should NOT have been rotated.
        assert backup_app.load_config()["router_api_token"] == "existing"

    async def test_set_repo_password_requires_auth(self, client):
        backup_app.ensure_default_config()
        resp = await client.post(
            "/api/config",
            data=json.dumps({"repo_password": "new-pw"}),
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 401

    async def test_invalid_interval_seconds(self, client):
        backup_app.ensure_default_config()
        resp = await client.post(
            "/api/config",
            data=json.dumps({"interval_seconds": "abc"}),
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 400
