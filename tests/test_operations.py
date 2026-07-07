"""Unit tests for OperationLock.

Focus: the stale-migration recovery that fixes issue #14 (a migration whose
source dies between receive_start and receive_finalize would otherwise hold the
lock until an app restart). Time is injected via ``now`` so these are
deterministic and don't sleep.
"""

from __future__ import annotations

from operations import OpKind, OperationLock


def test_acquire_release_tracks_state() -> None:
    lock = OperationLock()
    assert not lock.busy
    assert lock.idle_seconds() is None

    assert lock.try_acquire(OpKind.MIGRATION) is None
    assert lock.busy
    assert lock.migration_running
    assert lock.active is OpKind.MIGRATION

    lock.release(OpKind.MIGRATION)
    assert not lock.busy
    assert lock.idle_seconds() is None


def test_second_acquire_is_rejected_while_held() -> None:
    lock = OperationLock()
    assert lock.try_acquire(OpKind.MIGRATION) is None
    rejected = lock.try_acquire(OpKind.BACKUP)
    assert rejected == lock.busy_message()
    assert "migration" in rejected
    assert lock.migration_running


def test_busy_message_reflects_active_op() -> None:
    lock = OperationLock()
    assert lock.busy_message() is None  # idle
    lock.try_acquire(OpKind.BACKUP)
    msg = lock.busy_message()
    assert "backup" in msg and "paused" in msg
    lock.release(OpKind.BACKUP)
    assert lock.busy_message() is None


def test_on_change_fires_on_acquire_and_release() -> None:
    lock = OperationLock()
    events: list[str | None] = []
    lock.set_on_change(lambda: events.append(lock.active.value if lock.active else None))
    lock.try_acquire(OpKind.BACKUP)
    lock.release(OpKind.BACKUP)
    assert events == ["backup", None]  # acquire (active=backup), release (idle)


def test_on_change_not_fired_when_acquire_rejected() -> None:
    lock = OperationLock()
    lock.try_acquire(OpKind.BACKUP)
    calls: list[int] = []
    lock.set_on_change(lambda: calls.append(1))
    assert lock.try_acquire(OpKind.DELETE) is not None  # rejected
    assert calls == []  # no state change -> no notification


def test_on_change_callback_exception_is_swallowed() -> None:
    lock = OperationLock()

    def boom() -> None:
        raise RuntimeError("nope")

    lock.set_on_change(boom)
    # Must not propagate — a broken subscriber can't break the lock.
    assert lock.try_acquire(OpKind.BACKUP) is None
    lock.release(OpKind.BACKUP)


def test_release_if_stale_reclaims_idle_migration() -> None:
    lock = OperationLock()
    lock.try_acquire(OpKind.MIGRATION)
    lock.touch(now=1000.0)  # pin the activity clock

    # Still within the timeout -> left alone.
    assert lock.release_if_stale(OpKind.MIGRATION, 60, now=1059.0) is False
    assert lock.migration_running

    # Idle beyond the timeout -> reclaimed, no restart needed (issue #14).
    assert lock.release_if_stale(OpKind.MIGRATION, 60, now=1061.0) is True
    assert not lock.busy
    # After reclaim, a fresh operation can acquire the lock.
    assert lock.try_acquire(OpKind.BACKUP) is None


def test_release_if_stale_ignores_other_kinds() -> None:
    lock = OperationLock()
    lock.try_acquire(OpKind.BACKUP)
    lock.touch(now=1000.0)
    # The migration reclaim must not touch a long-running backup/restore.
    assert lock.release_if_stale(OpKind.MIGRATION, 60, now=9999.0) is False
    assert lock.backup_running


def test_release_if_stale_noop_when_nothing_held() -> None:
    lock = OperationLock()
    assert lock.release_if_stale(OpKind.MIGRATION, 60, now=9999.0) is False
    assert not lock.busy


def test_touch_keeps_live_transfer_fresh() -> None:
    lock = OperationLock()
    lock.try_acquire(OpKind.MIGRATION)
    lock.touch(now=1000.0)
    # A heartbeat at 1090 resets the idle clock...
    lock.touch(now=1090.0)
    # ...so at 1100 only 10s have elapsed -> a live migration is not reclaimed.
    assert lock.release_if_stale(OpKind.MIGRATION, 60, now=1100.0) is False
    assert lock.migration_running


def test_idle_seconds_measures_since_last_activity() -> None:
    lock = OperationLock()
    lock.try_acquire(OpKind.MIGRATION)
    lock.touch(now=1000.0)
    assert lock.idle_seconds(now=1042.0) == 42.0
