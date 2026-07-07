"""Centralised mutual-exclusion lock for long-running operations.

Instead of scattering ``backup_running`` / ``restore_running`` /
``migration_running`` booleans throughout the codebase (and having every
caller manually check all three), this module provides a single
``OperationLock`` that enforces the invariant: **at most one destructive
operation may run at a time**.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class OpKind(Enum):
    BACKUP = "backup"
    RESTORE = "restore"
    MIGRATION = "migration"
    # Manual removal of a single snapshot (restic forget --prune <id>).
    DELETE = "delete"
    # Background retention prune worker (restic prune after keep-* forget).
    PRUNE = "prune"


@dataclass
class OperationLock:
    """Simple cooperative lock for async-but-single-threaded Quart.

    Because Quart (like asyncio in general) is single-threaded, we don't
    need a real ``asyncio.Lock`` — a plain boolean is race-free as long as
    the check-and-set happens without an ``await`` in between.  This class
    wraps that pattern so callers can't forget.
    """

    _active: OpKind | None = field(default=None, init=False)
    _last_activity: float | None = field(default=None, init=False)
    # Called (no args) on every state transition — acquire and release — so a
    # push channel (SSE) can notify clients the instant the lock changes rather
    # than waiting for the next poll. Set via ``set_on_change``.
    _on_change: Callable[[], None] | None = field(default=None, init=False)

    def set_on_change(self, callback: Callable[[], None] | None) -> None:
        self._on_change = callback

    def _fire_change(self) -> None:
        if self._on_change is not None:
            try:
                self._on_change()
            except Exception:
                logger.exception("op_lock on_change callback failed")

    @property
    def active(self) -> OpKind | None:
        """The kind of operation currently running, or ``None``."""
        return self._active

    @property
    def busy(self) -> bool:
        return self._active is not None

    def busy_message(self) -> str | None:
        """User-facing explanation of the current operation, or ``None`` if idle.

        Single source of the wording used both by the UI status banner and by
        routes that reject a request because another operation holds the lock,
        so the two never drift.
        """
        if self._active is None:
            return None
        return (
            f"A {self._active.value} is in progress — other operations "
            f"are paused until it finishes."
        )

    def try_acquire(self, kind: OpKind) -> str | None:
        """Try to start *kind*.

        Returns ``None`` on success (the lock is now held) or an
        error-message string explaining why it couldn't be acquired.
        """
        if self._active is not None:
            return self.busy_message()
        self._active = kind
        self._last_activity = time.monotonic()
        self._fire_change()
        return None

    def release(self, kind: OpKind) -> None:
        """Release the lock.  Logs a warning on mismatch but always clears."""
        if self._active != kind:
            logger.warning(
                "OperationLock.release(%s) but active was %s", kind, self._active
            )
        self._active = None
        self._last_activity = None
        self._fire_change()

    def touch(self, *, now: float | None = None) -> None:
        """Mark activity on the held operation, refreshing its staleness clock.

        A migration streams data across many separate requests; calling this on
        each one keeps a live transfer from looking abandoned to
        ``release_if_stale``.  No-op when nothing is held.
        """
        if self._active is not None:
            self._last_activity = time.monotonic() if now is None else now

    def idle_seconds(self, *, now: float | None = None) -> float | None:
        """Seconds since the last activity, or ``None`` if nothing is held."""
        if self._last_activity is None:
            return None
        return (time.monotonic() if now is None else now) - self._last_activity

    def release_if_stale(
        self, kind: OpKind, max_idle_seconds: float, *, now: float | None = None
    ) -> bool:
        """Release the lock iff *kind* holds it and it has been idle too long.

        Guards against an operation abandoned without releasing — most notably a
        destination-side migration whose source died between ``receive_start``
        and ``receive_finalize`` (issue #14).  Returns ``True`` if a stale lock
        was cleared, ``False`` otherwise.
        """
        idle = self.idle_seconds(now=now)
        if self._active == kind and idle is not None and idle > max_idle_seconds:
            logger.warning(
                "Releasing stale %s lock (idle %.0fs > %.0fs)",
                kind.value,
                idle,
                max_idle_seconds,
            )
            self.release(kind)
            return True
        return False

    # Convenience read-only helpers so existing templates / status endpoints
    # can still ask "is a backup running?" etc.
    @property
    def backup_running(self) -> bool:
        return self._active == OpKind.BACKUP

    @property
    def restore_running(self) -> bool:
        return self._active == OpKind.RESTORE

    @property
    def migration_running(self) -> bool:
        return self._active == OpKind.MIGRATION

    @property
    def delete_running(self) -> bool:
        return self._active == OpKind.DELETE

    @property
    def prune_running(self) -> bool:
        return self._active == OpKind.PRUNE
