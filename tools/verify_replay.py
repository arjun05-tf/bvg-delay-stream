"""Replay verification.

Takes a content checksum of the aggregate tables and compares two labelled
snapshots. The checksum deliberately excludes updated_at: that column is wall
clock and must differ between runs. Everything else must match exactly, which
is the claim the replay proof makes.

Usage:
    python verify_replay.py capture first-run
    python verify_replay.py compare first-run replay
"""

from __future__ import annotations

import logging
import os
import sys

import psycopg

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
)
_log = logging.getLogger("verify_replay")

TABLES = {
    "delay_tumbling_1h": (
        "window_start::text || route_id || stop_id || event_count::text || "
        "sum_delay_seconds::text || min_delay_seconds::text || max_delay_seconds::text || "
        "early_count::text || on_time_count::text || late_count::text || very_late_count::text"
    ),
    "delay_sliding_1h_5m": (
        "window_start::text || route_id || stop_id || event_count::text || "
        "sum_delay_seconds::text || min_delay_seconds::text || max_delay_seconds::text || "
        "early_count::text || on_time_count::text || late_count::text || very_late_count::text"
    ),
    "late_events": (
        "observed_time::text || trip_id || start_date || stop_sequence::text || "
        "event_type::text || coalesce(delay_seconds::text, 'null') || lateness_seconds::text"
    ),
}


def checksum(connection: psycopg.Connection, table: str, expression: str) -> tuple[int, str]:
    """Compute a row count and an order independent content checksum.

    Args:
        connection: Open database connection.
        table: Table to check.
        expression: SQL expression that renders one row as text.

    Returns:
        A tuple of row count and checksum. The checksum of an empty table is
        the empty string, which keeps the comparison honest: two empty runs
        match, and that is a result worth seeing rather than hiding.
    """
    query = (
        f"SELECT count(*), coalesce(md5(string_agg(row_text, '|' ORDER BY row_text)), '') "
        f"FROM (SELECT {expression} AS row_text FROM {table}) AS rendered"
    )
    with connection.cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()
    assert row is not None
    return int(row[0]), str(row[1])


def capture(connection: psycopg.Connection, label: str) -> None:
    """Store checksums of every aggregate table under a label.

    Args:
        connection: Open database connection.
        label: Name for this snapshot, for example "first-run".
    """
    with connection.cursor() as cursor:
        for table, expression in TABLES.items():
            rows, digest = checksum(connection, table, expression)
            cursor.execute(
                "INSERT INTO replay_checksums (label, table_name, row_count, checksum, taken_at) "
                "VALUES (%s, %s, %s, %s, now()) "
                "ON CONFLICT (label, table_name) DO UPDATE SET "
                "row_count = EXCLUDED.row_count, checksum = EXCLUDED.checksum, taken_at = now()",
                (label, table, rows, digest),
            )
            _log.info("captured %s rows=%d checksum=%s", table, rows, digest or "(empty)")
    connection.commit()


def compare(connection: psycopg.Connection, left: str, right: str) -> bool:
    """Compare two labelled snapshots.

    Args:
        connection: Open database connection.
        left: First label.
        right: Second label.

    Returns:
        True when every table matches in both row count and checksum.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT table_name, label, row_count, checksum FROM replay_checksums "
            "WHERE label IN (%s, %s) ORDER BY table_name, label",
            (left, right),
        )
        rows = cursor.fetchall()

    by_table: dict[str, dict[str, tuple[int, str]]] = {}
    for table, label, count, digest in rows:
        by_table.setdefault(table, {})[label] = (count, digest)

    identical = True
    for table in TABLES:
        snapshots = by_table.get(table, {})
        if left not in snapshots or right not in snapshots:
            _log.error("%s: missing a snapshot (%s present)", table, sorted(snapshots))
            identical = False
            continue
        if snapshots[left] == snapshots[right]:
            _log.info(
                "%s: identical, rows=%d checksum=%s",
                table,
                snapshots[left][0],
                snapshots[left][1] or "(empty)",
            )
        else:
            _log.error(
                "%s: differs. %s=%s %s=%s", table, left, snapshots[left], right, snapshots[right]
            )
            identical = False
    return identical


def main(argv: list[str]) -> int:
    """Run the requested command.

    Args:
        argv: Command line arguments without the program name.

    Returns:
        Process exit code. 0 means the snapshots match or the capture
        succeeded; 1 means a difference, which is the signal CI acts on.
    """
    dsn = os.environ.get("PG_DSN", "")
    if not dsn:
        _log.error("PG_DSN is required")
        return 2
    if not argv or argv[0] not in {"capture", "compare"}:
        _log.error("usage: verify_replay.py capture <label> | compare <label> <label>")
        return 2

    with psycopg.connect(dsn) as connection:
        if argv[0] == "capture":
            if len(argv) != 2:
                _log.error("capture needs exactly one label")
                return 2
            capture(connection, argv[1])
            return 0
        if len(argv) != 3:
            _log.error("compare needs exactly two labels")
            return 2
        return 0 if compare(connection, argv[1], argv[2]) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
