"""Snapshot recorder.

Captures raw feed bodies to disk so the replay proof and the load test run
against real traffic instead of synthetic data, and so CI has fixtures that
do not need network access.
"""

from __future__ import annotations

import logging
from pathlib import Path

_log = logging.getLogger(__name__)


class SnapshotRecorder:
    """Writes raw feed snapshots to a directory, newest kept, oldest pruned."""

    def __init__(self, directory: Path, retention: int) -> None:
        """Create a recorder.

        Args:
            directory: Target directory. Created if missing.
            retention: Maximum number of snapshot files to keep. At one
                snapshot every 30 seconds, 2880 files is 24 hours.
        """
        self._directory = directory
        self._retention = retention
        self._directory.mkdir(parents=True, exist_ok=True)

    def record(self, payload: bytes, feed_timestamp: int) -> Path:
        """Write one snapshot.

        Args:
            payload: Raw feed body.
            feed_timestamp: Header timestamp, used as the file name so that
                lexical order equals chronological order.

        Returns:
            Path of the written file.
        """
        target = self._directory / f"{feed_timestamp:011d}.pb"
        target.write_bytes(payload)
        self._prune()
        return target

    def _prune(self) -> None:
        """Delete the oldest snapshots beyond the retention count."""
        files = sorted(self._directory.glob("*.pb"))
        excess = len(files) - self._retention
        if excess <= 0:
            # A negative count would slice from the end and delete everything
            # but the newest files, which is the opposite of the intent.
            return
        for stale in files[:excess]:
            try:
                stale.unlink()
            except OSError as exc:
                _log.warning(
                    "could not prune snapshot",
                    extra={"file": str(stale), "error": str(exc)},
                )
