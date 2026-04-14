"""Comprehensive tests for the migration module.

Covers:
- _tar_stream_sync: streaming tar creation with correct arcnames
- _streaming_tar_generator: async queue-based streaming with sentinel
- receive_all_data: extraction, path traversal blocking, invalid name filtering
- receive_finalize: fire-and-forget (no _wait_for_apps_ready), reload/deploy
- _build_manifest: manifest structure
- validate_name: valid/invalid names
- _strip_url_credentials: credential stripping
- run_direct_push: integration test with mocked httpx and router
"""

from __future__ import annotations

import asyncio
import gzip
import io
import os
import tarfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import migration
from migration import (
    MIGRATION_PROTOCOL_VERSION,
    _build_manifest,
    _strip_url_credentials,
    _streaming_tar_generator,
    _tar_stream_sync,
    receive_all_data,
    receive_all_data_from_file,
    receive_finalize,
    receive_start,
    run_direct_push,
    validate_name,
)
from operations import OpKind, OperationLock


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_app_data(tmp_path: Path) -> Path:
    """Create a realistic app_data directory with multiple app subdirs."""
    app_data = tmp_path / "app_data"
    app_data.mkdir()

    # App "myapp" with some files
    myapp = app_data / "myapp"
    myapp.mkdir()
    (myapp / "data.db").write_bytes(b"sqlite data here")
    sub = myapp / "uploads"
    sub.mkdir()
    (sub / "photo.jpg").write_bytes(b"\xff\xd8\xff" + b"\x00" * 100)

    # App "secrets" with nested structure
    secrets = app_data / "secrets"
    secrets.mkdir()
    (secrets / "vault.json").write_text('{"key": "value"}')
    nested = secrets / "certs"
    nested.mkdir()
    (nested / "server.pem").write_text("-----BEGIN CERTIFICATE-----\nfake\n")

    # App "agent-host" with a file
    agent = app_data / "agent-host"
    agent.mkdir()
    (agent / "config.yaml").write_text("host: 0.0.0.0\nport: 8080\n")

    return app_data


@pytest.fixture
def sample_apps() -> list[dict]:
    """Sample app metadata list."""
    return [
        {
            "name": "myapp",
            "repo_url": "https://user:token@github.com/org/myapp.git",
            "version": "1.0.0",
            "description": "My Application",
            "manifest_raw": '{"name": "myapp"}',
            "memory_mb": 512,
            "cpu_millicores": 500,
            "runtime_type": "docker",
            "status": "running",
        },
        {
            "name": "secrets",
            "repo_url": "https://github.com/org/secrets.git",
            "version": "2.1.0",
            "description": "Secrets Manager",
            "manifest_raw": None,
            "memory_mb": 256,
            "cpu_millicores": 250,
            "runtime_type": "docker",
            "status": "stopped",
        },
    ]


@pytest.fixture
def op_lock() -> OperationLock:
    """A fresh OperationLock for testing."""
    return OperationLock()


# ---------------------------------------------------------------------------
# validate_name
# ---------------------------------------------------------------------------


class TestValidateName:
    def test_valid_simple(self):
        assert validate_name("myapp") is True

    def test_valid_with_hyphens(self):
        assert validate_name("agent-host") is True

    def test_valid_with_dots(self):
        assert validate_name("app.v2") is True

    def test_valid_with_underscores(self):
        assert validate_name("my_app") is True

    def test_valid_with_colons_timestamps(self):
        assert validate_name("2024-01-15T10:30:00") is True

    def test_valid_alphanumeric_start(self):
        assert validate_name("A123") is True

    def test_invalid_empty(self):
        assert validate_name("") is False

    def test_invalid_too_long(self):
        assert validate_name("a" * 201) is False

    def test_invalid_path_traversal(self):
        assert validate_name("../etc/passwd") is False

    def test_invalid_slash(self):
        assert validate_name("my/app") is False

    def test_invalid_backslash(self):
        assert validate_name("my\\app") is False

    def test_invalid_double_dot(self):
        assert validate_name("..hidden") is False

    def test_invalid_starts_with_dot(self):
        assert validate_name(".hidden") is False

    def test_invalid_starts_with_hyphen(self):
        assert validate_name("-badname") is False

    def test_invalid_space(self):
        assert validate_name("my app") is False

    def test_max_length_valid(self):
        assert validate_name("a" * 200) is True


# ---------------------------------------------------------------------------
# _strip_url_credentials
# ---------------------------------------------------------------------------


class TestStripUrlCredentials:
    def test_strips_user_and_password(self):
        url = "https://user:token123@github.com/org/repo.git"
        result = _strip_url_credentials(url)
        assert result == "https://github.com/org/repo.git"

    def test_strips_user_only(self):
        url = "https://user@github.com/org/repo.git"
        result = _strip_url_credentials(url)
        assert result == "https://github.com/org/repo.git"

    def test_strips_access_token(self):
        url = "https://x-access-token:ghp_abc123@github.com/org/repo.git"
        result = _strip_url_credentials(url)
        assert result == "https://github.com/org/repo.git"

    def test_no_credentials_unchanged(self):
        url = "https://github.com/org/repo.git"
        result = _strip_url_credentials(url)
        assert result == "https://github.com/org/repo.git"

    def test_preserves_port(self):
        url = "https://user:pass@myhost.com:8443/path"
        result = _strip_url_credentials(url)
        assert result == "https://myhost.com:8443/path"

    def test_none_input(self):
        assert _strip_url_credentials(None) is None

    def test_empty_string(self):
        # Empty string doesn't start with "http", so returned as-is
        assert _strip_url_credentials("") == ""

    def test_non_http_url(self):
        url = "git@github.com:org/repo.git"
        result = _strip_url_credentials(url)
        assert result == url  # returned as-is (doesn't start with http)

    def test_preserves_query_and_fragment(self):
        url = "https://user:pass@example.com/path?foo=bar#section"
        result = _strip_url_credentials(url)
        assert result == "https://example.com/path?foo=bar#section"


# ---------------------------------------------------------------------------
# _build_manifest
# ---------------------------------------------------------------------------


class TestBuildManifest:
    def test_structure(self, sample_apps):
        manifest = _build_manifest(sample_apps, "test.example.com")
        assert manifest["version"] == MIGRATION_PROTOCOL_VERSION
        assert manifest["source_instance"] == "test.example.com"
        assert manifest["source_platform"] == "openhost"
        assert "created_at" in manifest
        assert isinstance(manifest["apps"], list)
        assert len(manifest["apps"]) == 2
        assert manifest["checksums"] == {}

    def test_app_fields(self, sample_apps):
        manifest = _build_manifest(sample_apps, "zone.example.com")
        app = manifest["apps"][0]
        assert app["name"] == "myapp"
        # repo_url should have credentials stripped
        assert app["repo_url"] == "https://github.com/org/myapp.git"
        assert app["version"] == "1.0.0"
        assert app["description"] == "My Application"
        assert app["memory_mb"] == 512
        assert app["cpu_millicores"] == 500
        assert app["runtime_type"] == "docker"
        assert app["status"] == "running"

    def test_strips_credentials_in_manifest(self, sample_apps):
        manifest = _build_manifest(sample_apps, "zone")
        # First app has user:token in URL
        assert "token" not in (manifest["apps"][0]["repo_url"] or "")
        assert "user" not in (manifest["apps"][0]["repo_url"] or "")

    def test_with_checksums(self, sample_apps):
        checksums = {"myapp": "sha256:abc123", "secrets": "sha256:def456"}
        manifest = _build_manifest(sample_apps, "zone", checksums=checksums)
        assert manifest["checksums"] == checksums

    def test_unknown_zone(self, sample_apps):
        # Empty zone_domain falls back to "unknown" via `or "unknown"`
        manifest = _build_manifest(sample_apps, "")
        assert manifest["source_instance"] == "unknown"

    def test_missing_optional_fields(self):
        """Apps with minimal metadata should still produce valid manifest entries."""
        apps = [{"name": "minimal"}]
        manifest = _build_manifest(apps, "z")
        app = manifest["apps"][0]
        assert app["name"] == "minimal"
        assert app["repo_url"] is None
        assert app["version"] is None
        assert app["manifest_raw"] is None


# ---------------------------------------------------------------------------
# _tar_stream_sync
# ---------------------------------------------------------------------------


class TestTarStreamSync:
    def test_creates_valid_tar_gz(self, tmp_app_data):
        """The function should produce a valid gzip-compressed tar archive."""
        read_fd, write_fd = os.pipe()
        accepted = ["myapp", "secrets"]

        # Run in a thread (mimics real usage)
        import threading

        t = threading.Thread(
            target=_tar_stream_sync,
            args=(tmp_app_data, accepted, write_fd),
        )
        t.start()

        # Read all data from the pipe
        with os.fdopen(read_fd, "rb") as f:
            data = f.read()
        t.join()

        # Verify it's a valid tar.gz
        buf = io.BytesIO(data)
        with tarfile.open(fileobj=buf, mode="r:gz") as tar:
            names = tar.getnames()
            # Should contain entries under myapp/ and secrets/
            assert any(n.startswith("myapp") for n in names)
            assert any(n.startswith("secrets") for n in names)

    def test_correct_arcnames(self, tmp_app_data):
        """Arcnames should be relative app names, not full filesystem paths."""
        read_fd, write_fd = os.pipe()
        accepted = ["myapp"]

        import threading

        t = threading.Thread(
            target=_tar_stream_sync,
            args=(tmp_app_data, accepted, write_fd),
        )
        t.start()

        with os.fdopen(read_fd, "rb") as f:
            data = f.read()
        t.join()

        buf = io.BytesIO(data)
        with tarfile.open(fileobj=buf, mode="r:gz") as tar:
            for member in tar.getmembers():
                # No entry should contain the full tmp_path
                assert str(tmp_app_data) not in member.name
                # All entries should start with the app name
                assert member.name.startswith("myapp")

    def test_skips_missing_app_dirs(self, tmp_app_data):
        """Apps that don't have a directory should be silently skipped."""
        read_fd, write_fd = os.pipe()
        accepted = ["myapp", "nonexistent-app", "secrets"]

        import threading

        t = threading.Thread(
            target=_tar_stream_sync,
            args=(tmp_app_data, accepted, write_fd),
        )
        t.start()

        with os.fdopen(read_fd, "rb") as f:
            data = f.read()
        t.join()

        buf = io.BytesIO(data)
        with tarfile.open(fileobj=buf, mode="r:gz") as tar:
            names = tar.getnames()
            assert any(n.startswith("myapp") for n in names)
            assert any(n.startswith("secrets") for n in names)
            assert not any("nonexistent" in n for n in names)

    def test_preserves_file_contents(self, tmp_app_data):
        """Extracted file contents should match the originals."""
        read_fd, write_fd = os.pipe()
        accepted = ["myapp"]

        import threading

        t = threading.Thread(
            target=_tar_stream_sync,
            args=(tmp_app_data, accepted, write_fd),
        )
        t.start()

        with os.fdopen(read_fd, "rb") as f:
            data = f.read()
        t.join()

        buf = io.BytesIO(data)
        with tarfile.open(fileobj=buf, mode="r:gz") as tar:
            member = tar.getmember("myapp/data.db")
            content = tar.extractfile(member).read()
            assert content == b"sqlite data here"

    def test_multiple_apps_all_included(self, tmp_app_data):
        """All three app dirs should be in the archive."""
        read_fd, write_fd = os.pipe()
        accepted = ["myapp", "secrets", "agent-host"]

        import threading

        t = threading.Thread(
            target=_tar_stream_sync,
            args=(tmp_app_data, accepted, write_fd),
        )
        t.start()

        with os.fdopen(read_fd, "rb") as f:
            data = f.read()
        t.join()

        buf = io.BytesIO(data)
        with tarfile.open(fileobj=buf, mode="r:gz") as tar:
            names = tar.getnames()
            top_dirs = {n.split("/")[0] for n in names}
            assert "myapp" in top_dirs
            assert "secrets" in top_dirs
            assert "agent-host" in top_dirs

    def test_empty_accepted_list(self, tmp_app_data):
        """An empty accepted list should produce an empty (but valid) tar.gz."""
        read_fd, write_fd = os.pipe()

        import threading

        t = threading.Thread(
            target=_tar_stream_sync,
            args=(tmp_app_data, [], write_fd),
        )
        t.start()

        with os.fdopen(read_fd, "rb") as f:
            data = f.read()
        t.join()

        buf = io.BytesIO(data)
        with tarfile.open(fileobj=buf, mode="r:gz") as tar:
            assert tar.getnames() == []


# ---------------------------------------------------------------------------
# _streaming_tar_generator
# ---------------------------------------------------------------------------


class TestStreamingTarGenerator:
    async def test_returns_chunks_and_sentinel(self, tmp_app_data):
        """Queue should yield byte chunks followed by a None sentinel."""
        queue = await _streaming_tar_generator(tmp_app_data, ["myapp"])

        chunks = []
        while True:
            chunk = await asyncio.wait_for(queue.get(), timeout=10)
            if chunk is None:
                break
            assert isinstance(chunk, bytes)
            assert len(chunk) > 0
            chunks.append(chunk)

        # Should have at least one data chunk
        assert len(chunks) >= 1

        # Reassembled data should be a valid tar.gz
        data = b"".join(chunks)
        buf = io.BytesIO(data)
        with tarfile.open(fileobj=buf, mode="r:gz") as tar:
            names = tar.getnames()
            assert any(n.startswith("myapp") for n in names)

    async def test_sentinel_terminates(self, tmp_app_data):
        """After None sentinel, the generator is done."""
        queue = await _streaming_tar_generator(tmp_app_data, ["myapp"])

        # Drain the queue
        sentinel_count = 0
        while True:
            chunk = await asyncio.wait_for(queue.get(), timeout=10)
            if chunk is None:
                sentinel_count += 1
                break

        assert sentinel_count == 1

    async def test_multiple_apps_stream(self, tmp_app_data):
        """Streaming multiple apps should produce a valid combined archive."""
        queue = await _streaming_tar_generator(
            tmp_app_data, ["myapp", "secrets", "agent-host"]
        )

        chunks = []
        while True:
            chunk = await asyncio.wait_for(queue.get(), timeout=10)
            if chunk is None:
                break
            chunks.append(chunk)

        data = b"".join(chunks)
        buf = io.BytesIO(data)
        with tarfile.open(fileobj=buf, mode="r:gz") as tar:
            top_dirs = {n.split("/")[0] for n in tar.getnames()}
            assert "myapp" in top_dirs
            assert "secrets" in top_dirs
            assert "agent-host" in top_dirs

    async def test_empty_app_list(self, tmp_app_data):
        """An empty app list should still terminate with None sentinel."""
        queue = await _streaming_tar_generator(tmp_app_data, [])

        chunk = await asyncio.wait_for(queue.get(), timeout=10)
        # Might get an empty tar chunk or go straight to sentinel
        while chunk is not None:
            chunk = await asyncio.wait_for(queue.get(), timeout=10)
        # Reached sentinel
        assert chunk is None


# ---------------------------------------------------------------------------
# receive_all_data
# ---------------------------------------------------------------------------


class TestReceiveAllData:
    def _make_tar_gz(
        self, tmp_path: Path, app_dirs: dict[str, dict[str, bytes]]
    ) -> bytes:
        """Helper to build a tar.gz in memory.

        app_dirs: {"appname": {"filename": b"content", ...}, ...}
        """
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            for app_name, files in app_dirs.items():
                app_dir = tmp_path / "src" / app_name
                app_dir.mkdir(parents=True, exist_ok=True)
                for fname, content in files.items():
                    fpath = app_dir / fname
                    fpath.parent.mkdir(parents=True, exist_ok=True)
                    fpath.write_bytes(content)
                    arcname = f"{app_name}/{fname}"
                    tar.add(str(fpath), arcname=arcname)
        return buf.getvalue()

    async def test_extracts_multiple_apps(self, tmp_path):
        """Should extract multiple app directories correctly."""
        dest = tmp_path / "app_data"
        dest.mkdir()

        tar_data = self._make_tar_gz(
            tmp_path,
            {
                "myapp": {"data.db": b"database content"},
                "secrets": {"vault.json": b'{"secret": true}'},
            },
        )

        result = await receive_all_data(io.BytesIO(tar_data), dest)

        assert result["ok"] is True
        assert set(result["apps"]) == {"myapp", "secrets"}
        assert (dest / "myapp" / "data.db").read_bytes() == b"database content"
        assert (dest / "secrets" / "vault.json").read_bytes() == b'{"secret": true}'

    async def test_path_traversal_blocked(self, tmp_path):
        """Tar members with '..' in paths should be filtered out."""
        dest = tmp_path / "app_data"
        dest.mkdir()

        # Build a tar.gz with a path-traversal member
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            # Normal file
            info = tarfile.TarInfo(name="goodapp/data.txt")
            content = b"good data"
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))

            # Path traversal attempt
            info2 = tarfile.TarInfo(name="../../../etc/passwd")
            evil = b"root:x:0:0:"
            info2.size = len(evil)
            tar.addfile(info2, io.BytesIO(evil))

        result = await receive_all_data(io.BytesIO(buf.getvalue()), dest)

        assert result["ok"] is True
        # Only the good app should be extracted
        assert "goodapp" in result["apps"]
        assert (dest / "goodapp" / "data.txt").exists()
        # The traversal file should NOT exist
        assert not (tmp_path / "etc" / "passwd").exists()

    async def test_absolute_path_blocked(self, tmp_path):
        """Tar members with absolute paths should be filtered out."""
        dest = tmp_path / "app_data"
        dest.mkdir()

        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            # Normal entry
            info = tarfile.TarInfo(name="safeapp/file.txt")
            data = b"safe"
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

            # Absolute path attempt
            info2 = tarfile.TarInfo(name="/tmp/evil.txt")
            evil = b"evil"
            info2.size = len(evil)
            tar.addfile(info2, io.BytesIO(evil))

        result = await receive_all_data(io.BytesIO(buf.getvalue()), dest)

        assert result["ok"] is True
        assert "safeapp" in result["apps"]

    async def test_invalid_app_names_filtered(self, tmp_path):
        """App names that fail validate_name should be skipped."""
        dest = tmp_path / "app_data"
        dest.mkdir()

        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            # Valid app
            info = tarfile.TarInfo(name="validapp/data.txt")
            data = b"valid"
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

            # Invalid app name (starts with dot)
            info2 = tarfile.TarInfo(name=".badapp/hack.txt")
            evil = b"bad"
            info2.size = len(evil)
            tar.addfile(info2, io.BytesIO(evil))

            # Invalid app name (starts with hyphen)
            info3 = tarfile.TarInfo(name="-another/file.txt")
            evil2 = b"bad2"
            info3.size = len(evil2)
            tar.addfile(info3, io.BytesIO(evil2))

        result = await receive_all_data(io.BytesIO(buf.getvalue()), dest)

        assert result["ok"] is True
        assert result["apps"] == ["validapp"]
        assert not (dest / ".badapp").exists()
        assert not (dest / "-another").exists()

    async def test_invalid_tar_returns_error(self, tmp_path):
        """Non-tar data should return an error result."""
        dest = tmp_path / "app_data"
        dest.mkdir()

        result = await receive_all_data(io.BytesIO(b"not a tar file"), dest)

        assert result["ok"] is False
        assert "error" in result

    async def test_with_stream_reader(self, tmp_path):
        """Test with io.BytesIO (simulating request body)."""
        dest = tmp_path / "app_data"
        dest.mkdir()

        tar_data = self._make_tar_gz(
            tmp_path,
            {
                "testapp": {"config.json": b'{"port": 8080}'},
            },
        )

        stream = io.BytesIO(tar_data)
        result = await receive_all_data(stream, dest)

        assert result["ok"] is True
        assert "testapp" in result["apps"]


# ---------------------------------------------------------------------------
# receive_finalize (fire-and-forget)
# ---------------------------------------------------------------------------


class TestReceiveFinalize:
    async def test_does_not_call_wait_for_apps_ready(self):
        """receive_finalize should NOT call _wait_for_apps_ready (removed)."""
        # Verify the function doesn't exist on the module
        assert not hasattr(migration, "_wait_for_apps_ready"), (
            "_wait_for_apps_ready should not exist in the migration module"
        )

    async def test_reloads_existing_apps(self):
        """Should call reload_app for apps that exist on the target."""
        manifest = {
            "apps": [
                {"name": "myapp", "status": "running", "repo_url": None},
            ]
        }

        with patch("migration._router_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = {"ok": True}
            result = await receive_finalize(
                manifest, "http://localhost:8080", "test-token"
            )

        assert result["ok"] is True
        # Should have called reload_app
        mock_post.assert_any_call(
            "/reload_app/myapp",
            token="test-token",
            base_url="http://localhost:8080",
        )

    async def test_stops_app_not_running_on_source(self):
        """Apps that were stopped on source should be stopped after reload."""
        manifest = {
            "apps": [
                {"name": "stoppedapp", "status": "stopped", "repo_url": None},
            ]
        }

        with patch("migration._router_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = {"ok": True}
            result = await receive_finalize(
                manifest, "http://localhost:8080", "test-token"
            )

        assert result["ok"] is True
        # Should have called reload then stop
        calls = [c.args[0] for c in mock_post.call_args_list]
        assert "/reload_app/stoppedapp" in calls
        assert "/stop_app/stoppedapp" in calls

    async def test_deploys_from_repo_url_when_reload_fails(self):
        """If reload fails (app doesn't exist), should deploy from repo_url."""
        manifest = {
            "apps": [
                {
                    "name": "newapp",
                    "status": "running",
                    "repo_url": "https://github.com/org/newapp.git",
                },
            ]
        }

        async def mock_post(path, data=None, token=None, base_url=""):
            if "/reload_app/" in path:
                raise Exception("app not found")
            return {"ok": True}

        with patch("migration._router_post", side_effect=mock_post):
            result = await receive_finalize(
                manifest,
                "http://localhost:8080",
                "test-token",
                repo_urls={"newapp": "https://token@github.com/org/newapp.git"},
            )

        assert result["ok"] is True
        deployed = [r for r in result["results"] if r["action"] == "deployed"]
        assert len(deployed) == 1
        assert deployed[0]["name"] == "newapp"

    async def test_prefers_repo_urls_over_manifest(self):
        """repo_urls dict (with credentials) should be preferred over manifest URLs."""
        manifest = {
            "apps": [
                {
                    "name": "app1",
                    "status": "running",
                    "repo_url": "https://github.com/org/app1.git",  # stripped
                },
            ]
        }

        call_args = {}

        async def mock_post(path, data=None, token=None, base_url=""):
            if "/reload_app/" in path:
                raise Exception("not found")
            if "/api/add_app" in path:
                call_args["data"] = data
            return {"ok": True}

        with patch("migration._router_post", side_effect=mock_post):
            await receive_finalize(
                manifest,
                "http://localhost:8080",
                "token",
                repo_urls={
                    "app1": "https://x-access-token:ghp_123@github.com/org/app1.git"
                },
            )

        # The deploy should use the credentialed URL
        assert (
            call_args["data"]["repo_url"]
            == "https://x-access-token:ghp_123@github.com/org/app1.git"
        )

    async def test_data_only_when_no_repo_url(self):
        """Apps without repo_url should be marked 'data_only'."""
        manifest = {
            "apps": [
                {"name": "dataapp", "status": "running", "repo_url": None},
            ]
        }

        async def mock_post(path, data=None, token=None, base_url=""):
            if "/reload_app/" in path:
                raise Exception("not found")
            return {"ok": True}

        with patch("migration._router_post", side_effect=mock_post):
            result = await receive_finalize(manifest, "http://localhost:8080", "token")

        assert result["ok"] is True
        assert result["results"][0]["action"] == "data_only"

    async def test_skips_backup_app(self):
        """The 'backup' app should be skipped."""
        manifest = {
            "apps": [
                {"name": "backup", "status": "running", "repo_url": None},
                {"name": "realapp", "status": "running", "repo_url": None},
            ]
        }

        with patch("migration._router_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = {"ok": True}
            result = await receive_finalize(manifest, "http://localhost:8080", "token")

        # Only realapp should appear in results
        names = [r["name"] for r in result["results"]]
        assert "backup" not in names
        assert "realapp" in names

    async def test_returns_apps_to_start(self):
        """Result should include apps_to_start for apps that were running."""
        manifest = {
            "apps": [
                {"name": "running1", "status": "running"},
                {"name": "stopped1", "status": "stopped"},
                {"name": "running2", "status": "running"},
            ]
        }

        with patch("migration._router_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = {"ok": True}
            result = await receive_finalize(manifest, "http://localhost:8080", "token")

        assert sorted(result["apps_to_start"]) == ["running1", "running2"]

    async def test_restarts_non_migrated_stopped_apps(self):
        """Apps stopped during receive_start that aren't being migrated should be restarted."""
        manifest = {
            "apps": [
                {"name": "migratedapp", "status": "running"},
            ]
        }

        # Simulate that receive_start stopped some apps
        migration._receive_stopped_apps = ["migratedapp", "otherliveapp"]

        with patch("migration._router_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = {"ok": True}
            result = await receive_finalize(manifest, "http://localhost:8080", "token")

        # otherliveapp should have been restarted (reload called)
        reload_calls = [
            c.args[0]
            for c in mock_post.call_args_list
            if "reload_app/otherliveapp" in c.args[0]
        ]
        assert len(reload_calls) == 1

        # _receive_stopped_apps should be cleared
        assert migration._receive_stopped_apps == []

    async def test_all_failed_returns_not_ok(self):
        """If all apps fail, result should have ok=False."""
        manifest = {
            "apps": [
                {
                    "name": "failapp1",
                    "status": "running",
                    "repo_url": "https://x.com/r.git",
                },
                {
                    "name": "failapp2",
                    "status": "running",
                    "repo_url": "https://x.com/r2.git",
                },
            ]
        }

        async def mock_post(path, data=None, token=None, base_url=""):
            raise Exception("everything is broken")

        with patch("migration._router_post", side_effect=mock_post):
            result = await receive_finalize(manifest, "http://localhost:8080", "token")

        assert result["ok"] is False


# ---------------------------------------------------------------------------
# run_direct_push (integration-style)
# ---------------------------------------------------------------------------


class TestRunDirectPush:
    async def test_three_step_protocol(self, tmp_app_data, op_lock):
        """run_direct_push should follow the 3-step protocol: start, data, finalize."""
        op_lock.try_acquire(OpKind.MIGRATION)

        posted_urls: list[str] = []

        # Mock get_apps_metadata to return known apps
        mock_apps = [
            {
                "name": "myapp",
                "repo_url": "https://github.com/org/myapp.git",
                "status": "running",
            },
        ]

        def make_response(status_code, json_data, content_type="application/json"):
            resp = MagicMock()
            resp.status_code = status_code
            resp.json.return_value = json_data
            resp.text = str(json_data)
            resp.headers = {"content-type": content_type}
            return resp

        async def mock_post(url, **kwargs):
            posted_urls.append(url)
            if "/receive/start" in url:
                return make_response(200, {"ok": True, "accepted_apps": ["myapp"]})
            elif "/receive/data" in url:
                # Consume the stream
                content = kwargs.get("content")
                if hasattr(content, "__aiter__"):
                    async for _ in content:
                        pass
                return make_response(200, {"ok": True, "message": "data received"})
            elif "/receive/finalize" in url:
                return make_response(
                    200, {"ok": True, "message": "finalized", "results": []}
                )
            return make_response(200, {"ok": True})

        mock_client = AsyncMock()
        mock_client.post = mock_post
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("migration.get_apps_metadata", new_callable=AsyncMock) as mock_meta,
            patch("migration.httpx.AsyncClient") as MockClient,
        ):
            mock_meta.return_value = mock_apps
            MockClient.return_value = mock_client

            result = await run_direct_push(
                target_url="https://target.example.com",
                target_token="target-token-123",
                selected_apps=None,
                lock=op_lock,
                all_app_data=tmp_app_data,
                vm_data_dir=tmp_app_data.parent / "vm_data",
                router_url="http://localhost:8080",
                zone_domain="source.example.com",
                router_token="src-token",
            )

        assert result is True

        # Verify all three endpoints were called
        assert any("/receive/start" in u for u in posted_urls)
        assert any("/receive/data" in u for u in posted_urls)
        assert any("/receive/finalize" in u for u in posted_urls)

        # Verify order: start < data < finalize
        start_idx = next(i for i, u in enumerate(posted_urls) if "/receive/start" in u)
        data_idx = next(i for i, u in enumerate(posted_urls) if "/receive/data" in u)
        finalize_idx = next(
            i for i, u in enumerate(posted_urls) if "/receive/finalize" in u
        )
        assert start_idx < data_idx < finalize_idx

    async def test_filters_backup_app(self, tmp_app_data, op_lock):
        """The 'backup' app should be excluded from migration."""
        op_lock.try_acquire(OpKind.MIGRATION)

        mock_apps = [
            {"name": "myapp", "repo_url": None, "status": "running"},
            {"name": "backup", "repo_url": None, "status": "running"},
        ]

        sent_manifests = []

        async def mock_post(url, **kwargs):
            resp = MagicMock()
            resp.status_code = 200
            resp.headers = {"content-type": "application/json"}

            if "/receive/start" in url:
                manifest = kwargs.get("json", {})
                sent_manifests.append(manifest)
                resp.json.return_value = {"ok": True, "accepted_apps": ["myapp"]}
            elif "/receive/data" in url:
                content = kwargs.get("content")
                if hasattr(content, "__aiter__"):
                    async for _ in content:
                        pass
                resp.json.return_value = {"ok": True, "message": "ok"}
            else:
                resp.json.return_value = {"ok": True, "message": "ok", "results": []}
            resp.text = "ok"
            return resp

        mock_client = AsyncMock()
        mock_client.post = mock_post
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("migration.get_apps_metadata", new_callable=AsyncMock) as mock_meta,
            patch("migration.httpx.AsyncClient") as MockClient,
        ):
            mock_meta.return_value = mock_apps
            MockClient.return_value = mock_client

            await run_direct_push(
                target_url="https://target.example.com",
                target_token="token",
                selected_apps=None,
                lock=op_lock,
                all_app_data=tmp_app_data,
                vm_data_dir=tmp_app_data.parent / "vm_data",
                router_url="http://localhost:8080",
                zone_domain="source.example.com",
                router_token="tok",
            )

        # Manifest should not include 'backup'
        manifest = sent_manifests[0]
        app_names = [a["name"] for a in manifest["apps"]]
        assert "backup" not in app_names
        assert "myapp" in app_names

    async def test_selected_apps_filter(self, tmp_app_data, op_lock):
        """When selected_apps is provided, only those apps should be migrated."""
        op_lock.try_acquire(OpKind.MIGRATION)

        mock_apps = [
            {"name": "myapp", "repo_url": None, "status": "running"},
            {"name": "secrets", "repo_url": None, "status": "running"},
            {"name": "agent-host", "repo_url": None, "status": "running"},
        ]

        sent_manifests = []

        async def mock_post(url, **kwargs):
            resp = MagicMock()
            resp.status_code = 200
            resp.headers = {"content-type": "application/json"}

            if "/receive/start" in url:
                manifest = kwargs.get("json", {})
                sent_manifests.append(manifest)
                resp.json.return_value = {"ok": True, "accepted_apps": ["myapp"]}
            elif "/receive/data" in url:
                content = kwargs.get("content")
                if hasattr(content, "__aiter__"):
                    async for _ in content:
                        pass
                resp.json.return_value = {"ok": True, "message": "ok"}
            else:
                resp.json.return_value = {"ok": True, "message": "ok", "results": []}
            resp.text = "ok"
            return resp

        mock_client = AsyncMock()
        mock_client.post = mock_post
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("migration.get_apps_metadata", new_callable=AsyncMock) as mock_meta,
            patch("migration.httpx.AsyncClient") as MockClient,
        ):
            mock_meta.return_value = mock_apps
            MockClient.return_value = mock_client

            await run_direct_push(
                target_url="https://target.example.com",
                target_token="token",
                selected_apps=["myapp"],
                lock=op_lock,
                all_app_data=tmp_app_data,
                vm_data_dir=tmp_app_data.parent / "vm_data",
                router_url="http://localhost:8080",
                zone_domain="zone",
                router_token="tok",
            )

        app_names = [a["name"] for a in sent_manifests[0]["apps"]]
        assert app_names == ["myapp"]

    async def test_returns_false_on_start_rejection(self, tmp_app_data, op_lock):
        """If the target rejects the manifest, run_direct_push returns False."""
        op_lock.try_acquire(OpKind.MIGRATION)

        mock_apps = [
            {"name": "myapp", "repo_url": None, "status": "running"},
        ]

        async def mock_post(url, **kwargs):
            resp = MagicMock()
            resp.headers = {"content-type": "application/json"}
            if "/receive/start" in url:
                resp.status_code = 400
                resp.text = "bad request"
                resp.json.return_value = {"ok": False, "error": "rejected"}
            else:
                resp.status_code = 200
                resp.json.return_value = {"ok": True}
            return resp

        mock_client = AsyncMock()
        mock_client.post = mock_post
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("migration.get_apps_metadata", new_callable=AsyncMock) as mock_meta,
            patch("migration.httpx.AsyncClient") as MockClient,
        ):
            mock_meta.return_value = mock_apps
            MockClient.return_value = mock_client

            result = await run_direct_push(
                target_url="https://target.example.com",
                target_token="token",
                selected_apps=None,
                lock=op_lock,
                all_app_data=tmp_app_data,
                vm_data_dir=tmp_app_data.parent / "vm_data",
                router_url="http://localhost:8080",
                zone_domain="zone",
                router_token="tok",
            )

        assert result is False
        # Lock should be released
        assert op_lock.active is None

    async def test_releases_lock_on_failure(self, tmp_app_data, op_lock):
        """Lock should always be released, even on failure."""
        op_lock.try_acquire(OpKind.MIGRATION)
        assert op_lock.active == OpKind.MIGRATION

        with patch("migration.get_apps_metadata", new_callable=AsyncMock) as mock_meta:
            mock_meta.side_effect = RuntimeError("connection failed")

            result = await run_direct_push(
                target_url="https://target.example.com",
                target_token="token",
                selected_apps=None,
                lock=op_lock,
                all_app_data=tmp_app_data,
                vm_data_dir=tmp_app_data.parent / "vm_data",
                router_url="http://localhost:8080",
                zone_domain="zone",
                router_token="tok",
            )

        assert result is False
        assert op_lock.active is None

    async def test_no_apps_returns_false(self, tmp_app_data, op_lock):
        """If no apps are found, run_direct_push should return False."""
        op_lock.try_acquire(OpKind.MIGRATION)

        with patch("migration.get_apps_metadata", new_callable=AsyncMock) as mock_meta:
            mock_meta.return_value = []

            result = await run_direct_push(
                target_url="https://target.example.com",
                target_token="token",
                selected_apps=None,
                lock=op_lock,
                all_app_data=tmp_app_data,
                vm_data_dir=tmp_app_data.parent / "vm_data",
                router_url="http://localhost:8080",
                zone_domain="zone",
                router_token="tok",
            )

        assert result is False
        assert op_lock.active is None


# ---------------------------------------------------------------------------
# OperationLock basics
# ---------------------------------------------------------------------------


class TestOperationLock:
    def test_acquire_and_release(self):
        lock = OperationLock()
        assert lock.active is None
        assert lock.busy is False

        result = lock.try_acquire(OpKind.MIGRATION)
        assert result is None  # success
        assert lock.active == OpKind.MIGRATION
        assert lock.busy is True
        assert lock.migration_running is True

        lock.release(OpKind.MIGRATION)
        assert lock.active is None
        assert lock.busy is False

    def test_mutual_exclusion(self):
        lock = OperationLock()
        lock.try_acquire(OpKind.BACKUP)

        err = lock.try_acquire(OpKind.MIGRATION)
        assert err is not None
        assert "backup" in err.lower()

    def test_convenience_properties(self):
        lock = OperationLock()
        lock.try_acquire(OpKind.BACKUP)
        assert lock.backup_running is True
        assert lock.restore_running is False
        assert lock.migration_running is False


# ---------------------------------------------------------------------------
# receive_start tests
# ---------------------------------------------------------------------------


class TestReceiveStart:
    """Tests for receive_start: manifest validation, app stopping, data wiping."""

    async def test_empty_apps_returns_error(self, tmp_app_data):
        manifest = {"apps": []}
        result = await receive_start(manifest, tmp_app_data)
        assert result["ok"] is False
        assert "No apps" in result["error"]

    async def test_no_apps_key_returns_error(self, tmp_app_data):
        manifest = {}
        result = await receive_start(manifest, tmp_app_data)
        assert result["ok"] is False

    async def test_invalid_app_names_filtered(self, tmp_app_data):
        manifest = {"apps": [{"name": "../evil"}, {"name": "/root"}]}
        result = await receive_start(manifest, tmp_app_data)
        assert result["ok"] is False
        assert "No valid" in result["error"]

    async def test_accepts_valid_apps(self, tmp_app_data):
        manifest = {
            "apps": [
                {"name": "myapp"},
                {"name": "secrets"},
            ]
        }
        result = await receive_start(manifest, tmp_app_data)
        assert result["ok"] is True
        assert set(result["accepted_apps"]) == {"myapp", "secrets"}

    async def test_deletes_existing_app_data(self, tmp_app_data):
        """receive_start should delete data for accepted apps."""
        myapp_dir = tmp_app_data / "myapp"
        assert myapp_dir.exists()
        assert (myapp_dir / "data.db").exists()

        manifest = {"apps": [{"name": "myapp"}]}
        result = await receive_start(manifest, tmp_app_data)
        assert result["ok"] is True
        # The data directory for myapp should be deleted
        assert not myapp_dir.exists()

    async def test_does_not_delete_non_accepted_apps(self, tmp_app_data):
        """Only accepted app dirs should be deleted."""
        manifest = {"apps": [{"name": "myapp"}]}
        await receive_start(manifest, tmp_app_data)
        # secrets dir should still exist
        assert (tmp_app_data / "secrets").exists()

    @patch("migration._router_get")
    @patch("migration._router_post")
    async def test_stops_running_apps(self, mock_post, mock_get, tmp_app_data):
        """Should stop all non-backup running apps on destination."""
        mock_get.return_value = {
            "myapp": {"status": "running"},
            "secrets": {"status": "running"},
            "backup": {"status": "running"},
        }
        mock_post.return_value = {"ok": True}

        manifest = {"apps": [{"name": "myapp"}]}
        result = await receive_start(
            manifest,
            tmp_app_data,
            router_url="http://localhost:8080",
            router_token="tok",
        )
        assert result["ok"] is True

        # Should have stopped myapp and secrets but NOT backup
        stop_calls = [c for c in mock_post.call_args_list if "/stop_app/" in str(c)]
        stopped_names = {str(c) for c in stop_calls}
        assert any("myapp" in s for s in stopped_names)
        assert any("secrets" in s for s in stopped_names)
        assert not any("backup" in s for s in stopped_names)

    async def test_records_stopped_apps(self, tmp_app_data):
        """Stopped apps should be recorded for receive_finalize to restart."""
        manifest = {"apps": [{"name": "myapp"}]}
        result = await receive_start(manifest, tmp_app_data)
        assert result["ok"] is True
        # stopped_apps should be in the result
        assert "stopped_apps" in result

    async def test_source_instance_logged(self, tmp_app_data):
        """The source instance from manifest should appear in logs."""
        migration.log.clear()
        manifest = {
            "apps": [{"name": "myapp"}],
            "source_instance": "test-source.example.com",
        }
        await receive_start(manifest, tmp_app_data)
        assert any("test-source.example.com" in entry for entry in migration.log)


# ---------------------------------------------------------------------------
# receive_all_data_from_file tests
# ---------------------------------------------------------------------------


class TestReceiveAllDataFromFile:
    """Tests for file-based tar extraction (avoids loading into memory)."""

    def _make_tar_file(self, tmp_path: Path, contents: dict[str, bytes]) -> str:
        """Create a tar.gz file on disk and return its path."""
        tar_path = str(tmp_path / "test.tar.gz")
        with tarfile.open(tar_path, mode="w:gz") as tar:
            for name, data in contents.items():
                info = tarfile.TarInfo(name=name)
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))
        return tar_path

    async def test_extracts_apps_from_file(self, tmp_path):
        dest = tmp_path / "app_data"
        dest.mkdir()
        tar_path = self._make_tar_file(
            tmp_path,
            {
                "myapp/data.db": b"db content",
                "secrets/sqlite/main.db": b"secret",
            },
        )
        result = await receive_all_data_from_file(tar_path, dest)
        assert result["ok"] is True
        assert "myapp" in result["apps"]
        assert "secrets" in result["apps"]
        assert (dest / "myapp" / "data.db").exists()
        assert (dest / "secrets" / "sqlite" / "main.db").exists()

    async def test_blocks_path_traversal(self, tmp_path):
        dest = tmp_path / "app_data"
        dest.mkdir()
        tar_path = self._make_tar_file(
            tmp_path,
            {"../evil/hack.txt": b"bad"},
        )
        result = await receive_all_data_from_file(tar_path, dest)
        assert result["ok"] is True
        # The evil path should have been filtered out
        assert not (tmp_path / "evil").exists()

    async def test_corrupt_file_returns_error(self, tmp_path):
        dest = tmp_path / "app_data"
        dest.mkdir()
        bad_path = str(tmp_path / "bad.tar.gz")
        Path(bad_path).write_bytes(b"not a tar file")
        result = await receive_all_data_from_file(bad_path, dest)
        assert result["ok"] is False
