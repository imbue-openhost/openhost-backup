"""Verify the backup app excludes the archive tier.

The archive tier (``/data/app_archive`` inside the container) is
mounted by openhost when the manifest opts into ``access_all_data``,
but the bytes shouldn't end up in restic snapshots — they live in
the archive backend (local persistent disk or S3 via JuiceFS) and
have their own durability story.
"""

from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("OPENHOST_APP_DATA_DIR", "/tmp/test_backup_excludes")
os.environ.setdefault("OPENHOST_APP_BASE_PATH", "/backup")

import app as backup_app


def test_app_archive_in_backup_excludes() -> None:
    """The archive mount is on the explicit exclude list."""
    assert Path("/data/app_archive") in backup_app.BACKUP_EXCLUDES


def test_backup_excludes_includes_self_repo_dir() -> None:
    """The backup app's own repo dir is also excluded so restic
    snapshots don't grow recursively when the repo is local.
    """
    assert backup_app.ALL_APP_DATA / "backup" in backup_app.BACKUP_EXCLUDES


def test_app_archive_not_in_backup_roots() -> None:
    """Belt-and-suspenders: ``/data/app_archive`` was never in
    BACKUP_ROOTS, and shouldn't be added there inadvertently.
    """
    assert Path("/data/app_archive") not in backup_app.BACKUP_ROOTS


def test_app_archive_not_in_root_names() -> None:
    """The restore UI surfaces ``_ROOT_NAMES`` to operators — the
    archive tier shouldn't be selectable as a restore target.
    """
    assert "app_archive" not in backup_app._ROOT_NAMES
    assert Path("/data/app_archive") not in backup_app._ROOT_NAMES.values()
