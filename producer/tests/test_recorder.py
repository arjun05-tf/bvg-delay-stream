"""Snapshot recorder tests."""

from __future__ import annotations

from pathlib import Path

from bvg_producer.recorder import SnapshotRecorder


def test_records_and_names_by_feed_timestamp(tmp_path: Path) -> None:
    recorder = SnapshotRecorder(tmp_path, retention=10)
    written = recorder.record(b"payload", 1_790_977_022)
    assert written.name == "01790977022.pb"
    assert written.read_bytes() == b"payload"


def test_retention_prunes_oldest_first(tmp_path: Path) -> None:
    recorder = SnapshotRecorder(tmp_path, retention=3)
    for offset in range(5):
        recorder.record(b"x", 1_790_977_000 + offset)
    remaining = sorted(path.name for path in tmp_path.glob("*.pb"))
    assert remaining == ["01790977002.pb", "01790977003.pb", "01790977004.pb"]
