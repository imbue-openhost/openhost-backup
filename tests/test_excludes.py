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


def test_backup_scope_summary_lists_archive_as_excluded() -> None:
    """The ``scope`` blob the index template renders must explicitly
    name ``/data/app_archive`` in its ``excluded`` list with a non-
    empty reason string.

    This is the load-bearing UI invariant for "make the exclusion
    visible" — if a future refactor drops the archive from the
    exclude list (by mistake or by removing the explicit ``--exclude``
    when the mount becomes optional), this test fails so the UI's
    promise to operators stays in lockstep with the code's behavior.
    """
    summary = backup_app._backup_scope_summary()

    excluded_paths = [entry["path"] for entry in summary["excluded"]]
    assert "/data/app_archive" in excluded_paths

    archive_entry = next(
        e for e in summary["excluded"] if e["path"] == "/data/app_archive"
    )
    assert archive_entry["reason"], (
        "Archive exclusion must carry a human-readable reason; "
        "without it the UI just shows a path with no context."
    )


def test_backup_scope_summary_lists_every_backup_root_in_included() -> None:
    """``scope.included`` must enumerate exactly ``BACKUP_ROOTS`` so
    the UI cannot diverge from what restic actually walks.  Pinning
    set-equality (rather than just containment) so adding a root to
    the code without surfacing it in the UI fails this test.
    """
    summary = backup_app._backup_scope_summary()
    included_paths = {entry["path"] for entry in summary["included"]}
    expected = {str(p) for p in backup_app.BACKUP_ROOTS}
    assert included_paths == expected


def test_backup_scope_summary_lists_every_exclude_in_excluded() -> None:
    """Mirror invariant on the exclude side: every entry in
    ``BACKUP_EXCLUDES`` must surface in the UI scope blob, so an
    exclude added to the code (e.g. for some future privileged
    mount) doesn't slip past operators.
    """
    summary = backup_app._backup_scope_summary()
    excluded_paths = {entry["path"] for entry in summary["excluded"]}
    expected = {str(p) for p in backup_app.BACKUP_EXCLUDES}
    assert excluded_paths == expected


def test_backup_scope_summary_marks_self_repo_as_implementation_detail() -> None:
    """The backup app's own restic-repo-containing data dir is an
    implementation detail (it has to be excluded so snapshots don't
    self-reference).  It must be flagged ``user_facing=False`` so
    the snapshots-browser note and the Migrate callout can
    suppress it without hard-coding the path on the JS side.

    Conversely, the archive tier must be ``user_facing=True`` —
    that's the whole point of surfacing it.
    """
    summary = backup_app._backup_scope_summary()
    by_path = {entry["path"]: entry for entry in summary["excluded"]}

    assert by_path["/data/app_data/backup"]["user_facing"] is False
    assert by_path["/data/app_archive"]["user_facing"] is True
