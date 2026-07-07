"""Integration tests for app.py HTTP routes related to migration.

Tests the Quart routes for the migration receive endpoints using the
Quart test client.
"""

from __future__ import annotations

import asyncio
import io
import json
import os
import tarfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

# Set required env vars before importing app
os.environ.setdefault("OPENHOST_APP_DATA_DIR", "/tmp/test_backup_data")
os.environ.setdefault("OPENHOST_APP_BASE_PATH", "/backup")

import app as backup_app


@pytest.fixture
def client(tmp_path):
    """Create a Quart test client with isolated data directories."""
    # Save originals so we can restore them after the test — other
    # test modules (test_excludes.py) rely on the module-level
    # constants retaining their import-time values.
    orig = {
        "ALL_APP_DATA": backup_app.ALL_APP_DATA,
        "APP_DATA_DIR": backup_app.APP_DATA_DIR,
        "CONFIG_DIR": backup_app.CONFIG_DIR,
        "DB_FILE": backup_app.DB_FILE,
        "CONFIG_FILE": backup_app.CONFIG_FILE,
        "RESTIC_REPO_DIR": backup_app.RESTIC_REPO_DIR,
    }

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

    yield backup_app.app.test_client()

    # Restore original module globals.
    for attr, val in orig.items():
        setattr(backup_app, attr, val)


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
        # Create a test file so os.walk has something to iterate.  Real files
        # in the test environment are owned by the test user (uid below the
        # subuid floor), so they are eligible for chown.
        (backup_app.ALL_APP_DATA / "testapp").mkdir(exist_ok=True)
        (backup_app.ALL_APP_DATA / "testapp" / "data.db").touch()

        backup_app.ROUTER_API_TOKEN = "test-token"
        response = await client.post("/api/chown-app-data")
        data = await response.get_json()
        assert data["ok"] is True
        assert data["count"] > 0
        assert data["skipped"] == 0
        # Verify chown was called with uid=1000, gid=1000
        for call_args in mock_chown.call_args_list:
            assert call_args[0][1] == 1000  # uid
            assert call_args[0][2] == 1000  # gid
        backup_app.ROUTER_API_TOKEN = ""

    @patch("os.chown")
    @patch("app._get_router_apps")
    async def test_chown_skips_subuid_mapped_files(
        self, mock_get, mock_chown, client
    ):
        """Files owned by a subuid-mapped user (uid >= 100000) must be left alone.

        Container apps that run a non-root in-container user (e.g. postgres at
        container uid 70) appear on the host as a subuid-shifted uid like
        165605.  Chowning those to 1000 destroys the user-namespace mapping,
        breaking the app.
        """
        mock_get.return_value = {
            "secrets": {"status": "stopped"},
            "backup": {"status": "running"},
        }
        # One ordinary file plus one subuid-mapped file under app_data.
        (backup_app.ALL_APP_DATA / "plane").mkdir(exist_ok=True)
        (backup_app.ALL_APP_DATA / "plane" / "postgres_conf").touch()
        (backup_app.ALL_APP_DATA / "plane" / "regular_file").touch()

        # Patch lstat *only* in the chown helper's dotted path.  os.walk uses
        # os.lstat too but goes through the C accelerator and is unaffected.
        real_lstat = os.lstat

        def _fake_lstat(path):
            st = real_lstat(path)
            if str(path).endswith("postgres_conf"):
                # Pretend this file is subuid-mapped on the host.
                return SimpleNamespace(
                    st_uid=165605, st_gid=165605, st_mode=st.st_mode
                )
            return st

        backup_app.ROUTER_API_TOKEN = "test-token"
        with patch("app.os.lstat", side_effect=_fake_lstat):
            response = await client.post("/api/chown-app-data")
        data = await response.get_json()
        assert data["ok"] is True
        assert data["skipped"] >= 1
        # The subuid-mapped path must never have been chown'd.
        chowned_paths = {call_args[0][0] for call_args in mock_chown.call_args_list}
        assert not any(p.endswith("postgres_conf") for p in chowned_paths)
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
    async def test_env_set_and_returned_plaintext(self, client):
        backup_app.ensure_default_config()

        resp = await client.post(
            "/api/config",
            data=json.dumps({"env": {"AWS_ACCESS_KEY_ID": "test-key"}}),
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 200
        assert (await resp.get_json())["ok"] is True

        resp2 = await client.get("/api/config")
        data2 = await resp2.get_json()
        # No redaction — the app is owner-only behind the router, so GET
        # returns what's saved.
        assert data2["config"]["env"]["AWS_ACCESS_KEY_ID"] == "test-key"
        assert backup_app.load_config()["env"]["AWS_ACCESS_KEY_ID"] == "test-key"

    async def test_env_accepts_arbitrary_keys(self, client):
        # No whitelist anymore — owner can set any env var restic needs.
        resp = await client.post(
            "/api/config",
            data=json.dumps({"env": {"B2_ACCOUNT_ID": "abc", "CUSTOM_KEY": "v"}}),
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 200
        conf = backup_app.load_config()
        assert conf["env"]["B2_ACCOUNT_ID"] == "abc"
        assert conf["env"]["CUSTOM_KEY"] == "v"

    async def test_env_post_replaces_rather_than_merging(self, client):
        # The form posts the entire env block on every save; older keys
        # must disappear when omitted, not silently linger.
        backup_app.ensure_default_config()
        conf = backup_app.load_config()
        conf["env"] = {"OLD_KEY": "stale", "AWS_ACCESS_KEY_ID": "k"}
        backup_app.save_config(conf)

        resp = await client.post(
            "/api/config",
            data=json.dumps({"env": {"AWS_ACCESS_KEY_ID": "k"}}),
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 200
        env = backup_app.load_config().get("env") or {}
        assert env == {"AWS_ACCESS_KEY_ID": "k"}

    async def test_env_empty_values_dropped(self, client):
        # Submitting "" for a key in the env dict drops the key rather
        # than persisting an empty string (which restic would still
        # see in os.environ).
        backup_app.ensure_default_config()
        conf = backup_app.load_config()
        conf["env"] = {"AWS_ACCESS_KEY_ID": "to-be-cleared"}
        backup_app.save_config(conf)

        resp = await client.post(
            "/api/config",
            data=json.dumps({"env": {"AWS_ACCESS_KEY_ID": ""}}),
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 200
        assert "AWS_ACCESS_KEY_ID" not in (backup_app.load_config().get("env") or {})


class TestStatusBackend:
    async def test_status_includes_backend(self, client):
        backup_app.ensure_default_config()
        resp = await client.get("/api/status")
        data = await resp.get_json()
        assert "backend" in data
        # Default config has no repo configured.
        assert data["backend"]["type"] == "unknown"
        assert data["backend"]["remote"] is False


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

    async def test_set_repo_password_no_auth_required(self, client):
        # The owner is the only caller (the app has no public paths), so
        # writing repo_password is no longer gated.
        backup_app.ensure_default_config()
        resp = await client.post(
            "/api/config",
            data=json.dumps({"repo_password": "new-pw"}),
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 200
        assert backup_app.load_config()["repo_password"] == "new-pw"

    async def test_invalid_interval_seconds(self, client):
        backup_app.ensure_default_config()
        resp = await client.post(
            "/api/config",
            data=json.dumps({"interval_seconds": "abc"}),
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 400


class TestEnsureRepoInitialized:
    """``ensure_repo_initialized`` is the single entry point that decides
    whether to ``restic init`` an unconfigured repo.  Three behaviours
    must hold:

    1. ``auto_init=True`` always inits on a "not initialized" signal.
    2. ``auto_init=False`` never inits and reports a clear error.
    3. ``auto_init=None`` only inits when ``classify_repo(...)`` says the
       repo is local — protecting against typo'd remote URLs creating
       empty repositories at the wrong location (S3 buckets, SFTP paths,
       etc.) when an end-user clicks a read-only UI button.
    """

    @staticmethod
    def _patch_run_restic(monkeypatch, sequence):
        """Replace _run_restic with a callable that yields the next entry
        from ``sequence`` on each invocation.  Each entry is a tuple of
        (returncode, stdout_bytes, stderr_bytes)."""
        calls: list[list[str]] = []
        it = iter(sequence)

        async def fake(args, conf, timeout=None):
            calls.append(list(args))
            return next(it)

        monkeypatch.setattr(backup_app, "_run_restic", fake)
        return calls

    async def test_returns_ready_when_cat_config_succeeds(self, monkeypatch):
        self._patch_run_restic(monkeypatch, [(0, b"", b"")])
        initialized_now, err = await backup_app.ensure_repo_initialized(
            {"repo": "/tmp/x", "repo_password": "p"}
        )
        assert initialized_now is False
        assert err is None

    async def test_local_auto_inits_by_default(self, monkeypatch, tmp_path):
        repo = tmp_path / "repo"
        calls = self._patch_run_restic(
            monkeypatch,
            [
                (1, b"", b"unable to open config file"),  # cat config
                (0, b"", b""),  # init
            ],
        )
        initialized_now, err = await backup_app.ensure_repo_initialized(
            {"repo": str(repo), "repo_password": "p"}
        )
        assert initialized_now is True
        assert err is None
        assert calls == [["cat", "config", "--no-lock"], ["init"]]

    async def test_remote_does_not_auto_init_by_default(self, monkeypatch):
        calls = self._patch_run_restic(
            monkeypatch,
            [(1, b"", b"Fatal: unable to open config file")],
        )
        initialized_now, err = await backup_app.ensure_repo_initialized(
            {"repo": "s3:s3.amazonaws.com/my-bucket/typo", "repo_password": "p"}
        )
        assert initialized_now is False
        assert err is not None
        assert "not initialized" in err.lower()
        # Must NOT have invoked restic init.
        assert calls == [["cat", "config", "--no-lock"]]

    async def test_remote_inits_when_explicitly_opted_in(self, monkeypatch):
        calls = self._patch_run_restic(
            monkeypatch,
            [
                (1, b"", b"unable to open config file"),
                (0, b"", b""),
            ],
        )
        initialized_now, err = await backup_app.ensure_repo_initialized(
            {"repo": "s3:s3.amazonaws.com/bucket/path", "repo_password": "p"},
            auto_init=True,
        )
        assert initialized_now is True
        assert err is None
        assert calls == [["cat", "config", "--no-lock"], ["init"]]

    async def test_auto_init_false_never_inits_local_either(
        self, monkeypatch, tmp_path
    ):
        calls = self._patch_run_restic(
            monkeypatch,
            [(1, b"", b"unable to open config file")],
        )
        initialized_now, err = await backup_app.ensure_repo_initialized(
            {"repo": str(tmp_path / "repo"), "repo_password": "p"},
            auto_init=False,
        )
        assert initialized_now is False
        assert err is not None
        assert "not initialized" in err.lower()
        assert calls == [["cat", "config", "--no-lock"]]

    async def test_non_init_error_passes_through(self, monkeypatch):
        # e.g. wrong password — must NOT auto-init regardless of mode.
        self._patch_run_restic(
            monkeypatch,
            [(1, b"", b"wrong password or no key found")],
        )
        initialized_now, err = await backup_app.ensure_repo_initialized(
            {"repo": "/tmp/x", "repo_password": "p"}
        )
        assert initialized_now is False
        assert err is not None
        assert "wrong password" in err.lower()


class TestIndexRendersScope:
    """End-to-end render tests for the index page's new scope panel.

    These tests exercise the full Quart render path so that template
    syntax errors, missing context vars, or broken Jinja loops fail
    loudly here rather than only surfacing in production after a
    deploy.
    """

    async def test_index_renders_archive_exclusion_in_status_panel(self, client):
        """The Status panel's <details> block must mention
        ``/data/app_archive`` and explain the exclusion in
        operator-readable language.  Sufficient pinning so a
        future template refactor that drops the panel fails this
        test rather than silently shipping a less-informative UI.
        """
        resp = await client.get("/")
        assert resp.status_code == 200
        body = (await resp.get_data()).decode()
        assert "What is and isn't backed up" in body
        assert "/data/app_archive" in body
        assert "intentionally excluded" in body or "intentionally not captured" in body

    async def test_index_renders_archive_exclusion_in_migrate_section(self, client):
        """The Migrate tab's "Important details" callout also names
        the archive exclusion so an operator reading the migration
        warning understands the destination won't have archive data.
        """
        resp = await client.get("/")
        assert resp.status_code == 200
        body = (await resp.get_data()).decode()
        # Both the migrate callout and the file-browser note should
        # reference the archive path; this asserts the migrate path
        # specifically by anchoring on the surrounding migrate copy.
        assert "Not migrated" in body
        # The migrate paragraph itself names the archive path within
        # the same DOM node, which is how the operator sees it.
        idx = body.find("Not migrated")
        # Allow up to ~600 chars after "Not migrated" for the rest of
        # the paragraph to mention the archive path.
        assert "/data/app_archive" in body[idx : idx + 1200]

    async def test_index_renders_every_backup_root(self, client):
        """Every BACKUP_ROOTS path must appear in the rendered page.
        This is the lockstep guarantee the scope summary makes — the
        UI shows exactly what restic will walk.
        """
        conf = backup_app.load_config()
        conf["repo"] = "/tmp/test-repo"
        backup_app.save_config(conf)
        resp = await client.get("/")
        body = (await resp.get_data()).decode()
        for root in backup_app.BACKUP_ROOTS:
            assert str(root) in body, f"missing BACKUP_ROOTS path {root}"


class TestRepoStatsCache:
    """The /api/repo/stats cache: stamped by backup/delete, served on read.

    The cache is written by folding stats into each mutating path's own DB
    write (``record_backup`` for a backup, ``delete_snapshot`` for a prune),
    so there is no standalone save helper to test in isolation.
    """

    STATS = {
        "total_size_bytes": 4_200_000_000,
        "total_uncompressed_size_bytes": 8_800_000_000,
        "total_blob_count": 12345,
        "snapshots_count": 37,
        "compression_ratio": 2.1,
    }

    def _record_backup(self, repo_stats=None, snapshot_id="a" * 64):
        backup_app.record_backup(
            "2026-01-01T00:00:00",
            "success",
            snapshot_id=snapshot_id,
            name="t",
            repo_stats=repo_stats,
        )

    def test_record_backup_folds_stats_into_row(self, client):
        # The backup path stamps the cache in its own INSERT.
        self._record_backup(repo_stats=self.STATS)
        loaded = backup_app.load_repo_stats_cache()
        assert loaded is not None
        for k, v in self.STATS.items():
            assert loaded[k] == v
        assert loaded["computed_at"]  # timestamp stamped

    def test_load_returns_none_without_stamp(self, client):
        # A backup row exists but carried no repo stats.
        self._record_backup()
        assert backup_app.load_repo_stats_cache() is None

    async def test_config_repo_change_invalidates_cache(self, client):
        # Changing the repo URL must drop the cache (it described the old repo).
        conf = backup_app.load_config()
        conf["repo"] = "s3:old"
        conf["repo_password"] = "p"
        backup_app.save_config(conf)
        self._record_backup(repo_stats=self.STATS)
        assert backup_app.load_repo_stats_cache() is not None
        resp = await client.post("/api/config", json={"repo": "s3:new"})
        assert resp.status_code == 200
        assert backup_app.load_repo_stats_cache() is None

    async def test_config_non_repo_change_keeps_cache(self, client):
        # Changing a non-repo field leaves the cache intact.
        conf = backup_app.load_config()
        conf["repo"] = "s3:same"
        conf["repo_password"] = "p"
        backup_app.save_config(conf)
        self._record_backup(repo_stats=self.STATS)
        resp = await client.post("/api/config", json={"interval_seconds": 120})
        assert resp.status_code == 200
        assert backup_app.load_repo_stats_cache() is not None
        # Re-saving the same repo URL is a no-op for the cache too.
        resp = await client.post("/api/config", json={"repo": "s3:same"})
        assert resp.status_code == 200
        assert backup_app.load_repo_stats_cache() is not None

    def test_load_returns_newest_stamped_row(self, client):
        # An unstamped newer backup must not shadow the last known-good stats.
        self._record_backup(repo_stats=self.STATS, snapshot_id="a" * 64)
        self._record_backup(repo_stats=None, snapshot_id="b" * 64)
        loaded = backup_app.load_repo_stats_cache()
        assert loaded is not None
        assert loaded["snapshots_count"] == 37

    async def test_api_serves_cache_without_recompute(self, client):
        self._record_backup(repo_stats=self.STATS)
        with patch.object(backup_app, "repo_stats", new=AsyncMock()) as mock_stats:
            resp = await client.get("/api/repo/stats")
            body = await resp.get_json()
        assert resp.status_code == 200
        assert body["ok"] is True
        assert body["cached"] is True
        assert body["stats"]["total_size_bytes"] == self.STATS["total_size_bytes"]
        mock_stats.assert_not_called()  # served from DB, restic never invoked

    async def test_api_computes_live_when_no_cache(self, client):
        self._record_backup(repo_stats=None)  # row exists but unstamped
        with patch.object(
            backup_app, "repo_stats", new=AsyncMock(return_value=(self.STATS, None))
        ) as mock_stats:
            resp = await client.get("/api/repo/stats")
            body = await resp.get_json()
        assert resp.status_code == 200
        assert body["cached"] is False
        assert body["stats"]["snapshots_count"] == 37
        mock_stats.assert_called_once()
        # The live read is not persisted — cache updates only on backup/delete.
        assert backup_app.load_repo_stats_cache() is None

    async def test_api_refresh_bypasses_cache(self, client):
        self._record_backup(repo_stats=self.STATS)
        fresh = {**self.STATS, "total_size_bytes": 5_000_000_000, "snapshots_count": 38}
        with patch.object(
            backup_app, "repo_stats", new=AsyncMock(return_value=(fresh, None))
        ) as mock_stats:
            resp = await client.get("/api/repo/stats?refresh=1")
            body = await resp.get_json()
        assert resp.status_code == 200
        assert body["cached"] is False
        assert body["stats"]["total_size_bytes"] == 5_000_000_000
        mock_stats.assert_called_once()
        # Bypass is one-off: the stored cache still holds the old value.
        assert backup_app.load_repo_stats_cache()["total_size_bytes"] == (
            self.STATS["total_size_bytes"]
        )

    async def test_api_error_when_no_cache_and_compute_fails(self, client):
        with patch.object(
            backup_app, "repo_stats", new=AsyncMock(return_value=(None, "boom"))
        ):
            resp = await client.get("/api/repo/stats")
            body = await resp.get_json()
        assert resp.status_code == 500
        assert body["ok"] is False
        assert body["error"] == "boom"

    async def test_delete_snapshot_restamps_surviving_row(self, client):
        # Two stamped backups; deleting the newer removes its row and re-stamps
        # the survivor with freshly recomputed stats (refresh awaited inline
        # under the DELETE lock). delete_snapshot must also leave op_lock free.
        self._record_backup(repo_stats=self.STATS, snapshot_id="a" * 64)
        self._record_backup(repo_stats=self.STATS, snapshot_id="b" * 64)
        conf = backup_app.load_config()
        conf["repo"] = "/tmp/test-repo"
        conf["repo_password"] = "p"
        backup_app.save_config(conf)
        after_prune = {**self.STATS, "total_size_bytes": 1_000_000_000, "snapshots_count": 1}
        with patch.object(
            backup_app, "_run_restic", new=AsyncMock(return_value=(0, b"", b""))
        ), patch.object(
            backup_app, "repo_stats", new=AsyncMock(return_value=(after_prune, None))
        ):
            ok = await backup_app.delete_snapshot("b" * 64)
        assert ok is True
        assert not backup_app.op_lock.busy  # lock released after the delete
        loaded = backup_app.load_repo_stats_cache()
        assert loaded["total_size_bytes"] == 1_000_000_000
        assert loaded["snapshots_count"] == 1

    async def test_delete_snapshot_rejected_when_busy(self, client):
        # A delete must not proceed while another operation holds op_lock.
        conf = backup_app.load_config()
        conf["repo"] = "/tmp/test-repo"
        conf["repo_password"] = "p"
        backup_app.save_config(conf)
        backup_app.op_lock.try_acquire(backup_app.OpKind.BACKUP)
        try:
            with patch.object(
                backup_app, "_run_restic", new=AsyncMock(return_value=(0, b"", b""))
            ) as mock_restic:
                ok = await backup_app.delete_snapshot("a" * 64)
            assert ok is False
            mock_restic.assert_not_called()  # never ran the prune
        finally:
            backup_app.op_lock.release(backup_app.OpKind.BACKUP)

    async def test_refresh_repo_stats_cache_restamps_newest(self, client):
        # The background helper on its own: recompute + re-stamp the newest row.
        self._record_backup(repo_stats=self.STATS, snapshot_id="a" * 64)
        fresh = {**self.STATS, "total_size_bytes": 1_000_000_000, "snapshots_count": 1}
        with patch.object(
            backup_app, "repo_stats", new=AsyncMock(return_value=(fresh, None))
        ):
            await backup_app._refresh_repo_stats_cache()
        loaded = backup_app.load_repo_stats_cache()
        assert loaded["total_size_bytes"] == 1_000_000_000
        assert loaded["snapshots_count"] == 1

    async def test_refresh_repo_stats_cache_noop_on_failure(self, client):
        # If stats can't be computed, the previous stamp is left untouched.
        self._record_backup(repo_stats=self.STATS, snapshot_id="a" * 64)
        with patch.object(
            backup_app, "repo_stats", new=AsyncMock(return_value=(None, "boom"))
        ):
            await backup_app._refresh_repo_stats_cache()
        loaded = backup_app.load_repo_stats_cache()
        assert loaded["total_size_bytes"] == self.STATS["total_size_bytes"]


class TestCheckHoldsLock:
    """`restic check` takes a repo lock, so run_check must hold op_lock(CHECK)
    for its duration — upholding the invariant that op_lock is held whenever a
    restic lock is held."""

    def _configure(self):
        conf = backup_app.load_config()
        conf["repo"] = "/tmp/test-repo"
        conf["repo_password"] = "p"
        backup_app.save_config(conf)

    async def test_check_rejected_when_busy(self, client):
        self._configure()
        backup_app.op_lock.try_acquire(backup_app.OpKind.BACKUP)
        try:
            with patch.object(
                backup_app, "_run_restic", new=AsyncMock(return_value=(0, b"", b""))
            ) as mock_restic:
                ok = await backup_app.run_check()
            assert ok is False
            assert backup_app.check_last_status == "error"
            mock_restic.assert_not_called()  # never ran restic check
        finally:
            backup_app.op_lock.release(backup_app.OpKind.BACKUP)

    async def test_check_holds_lock_then_releases(self, client):
        self._configure()
        seen = {}

        async def fake_run(args, conf, timeout=None):
            # cat config probe -> repo exists; check -> record the active op.
            if args and args[0] == "check":
                seen["active"] = backup_app.op_lock.active
            return 0, b"", b""

        with patch.object(backup_app, "_run_restic", new=fake_run):
            ok = await backup_app.run_check()
        assert ok is True
        assert seen["active"] == backup_app.OpKind.CHECK  # held during the check
        assert not backup_app.op_lock.busy  # released afterward


class TestStatusPush:
    """The SSE push mechanism behind the live status banner."""

    def test_lock_status_reflects_op_lock(self, client):
        assert backup_app._lock_status()["busy"] is False
        backup_app.op_lock.try_acquire(backup_app.OpKind.DELETE)
        try:
            s = backup_app._lock_status()
            assert s["busy"] is True
            assert s["active_op"] == "delete"
            assert "delete" in s["busy_message"]
        finally:
            backup_app.op_lock.release(backup_app.OpKind.DELETE)

    def test_notify_wakes_subscribers(self, client):
        q: asyncio.Queue = asyncio.Queue()
        backup_app._status_subscribers.add(q)
        try:
            backup_app._notify_status_change()
            assert q.qsize() == 1
        finally:
            backup_app._status_subscribers.discard(q)

    def test_notify_tolerates_full_subscriber_queue(self, client):
        q: asyncio.Queue = asyncio.Queue(maxsize=1)
        q.put_nowait(None)  # already full
        backup_app._status_subscribers.add(q)
        try:
            backup_app._notify_status_change()  # must not raise
            assert q.qsize() == 1
        finally:
            backup_app._status_subscribers.discard(q)
