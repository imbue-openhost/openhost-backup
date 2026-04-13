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
    backup_app.RCLONE_CONF = backup_app.APP_DATA_DIR / "rclone.conf"
    backup_app.CONFIG_FILE = backup_app.APP_DATA_DIR / "config.json"

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


class TestHealthEndpoint:
    async def test_health(self, client):
        response = await client.get("/health")
        assert response.status_code == 200
