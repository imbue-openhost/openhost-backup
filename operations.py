"""Centralised mutual-exclusion lock for long-running operations.

Instead of scattering ``backup_running`` / ``restore_running`` /
``migration_running`` booleans throughout the codebase (and having every
caller manually check all three), this module provides a single
``OperationLock`` that enforces the invariant: **at most one destructive
operation may run at a time**.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class OpKind(Enum):
    BACKUP = "backup"
    RESTORE = "restore"
    MIGRATION = "migration"


@dataclass
class OperationLock:
    """Simple cooperative lock for async-but-single-threaded Quart.

    Because Quart (like asyncio in general) is single-threaded, we don't
    need a real ``asyncio.Lock`` — a plain boolean is race-free as long as
    the check-and-set happens without an ``await`` in between.  This class
    wraps that pattern so callers can't forget.
    """

    _active: OpKind | None = field(default=None, init=False)

    @property
    def active(self) -> OpKind | None:
        """The kind of operation currently running, or ``None``."""
        return self._active

    @property
    def busy(self) -> bool:
        return self._active is not None

    def try_acquire(self, kind: OpKind) -> str | None:
        """Try to start *kind*.

        Returns ``None`` on success (the lock is now held) or an
        error-message string explaining why it couldn't be acquired.
        """
        if self._active is not None:
            return f"{self._active.value} in progress"
        self._active = kind
        return None

    def release(self, kind: OpKind) -> None:
        """Release the lock.  Logs a warning on mismatch but always clears."""
        if self._active != kind:
            logger.warning(
                "OperationLock.release(%s) but active was %s", kind, self._active
            )
        self._active = None

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
