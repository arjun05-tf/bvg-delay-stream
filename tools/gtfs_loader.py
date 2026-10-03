"""Load the GTFS Schedule dimension tables.

Only stops.txt and routes.txt are imported. stop_times.txt for the whole VBB
network is on the order of tens of millions of rows and several gigabytes of
index; it does not belong on a laptop and the pipeline does not need it,
because the realtime feed carries its own delay against the schedule. See
docs/architecture.md, "Why delay comes from the feed".

Runs to completion and exits. Safe to run repeatedly: every row is upserted.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import sys
import tempfile
import zipfile
from collections.abc import Iterator
from pathlib import Path

import psycopg
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
)
_log = logging.getLogger("gtfs_loader")

DEFAULT_GTFS_URL = "https://unternehmen.vbb.de/gtfs"
CHUNK_BYTES = 1 << 20
BATCH_ROWS = 5_000


def download(url: str, target: Path, timeout: int) -> None:
    """Stream the GTFS archive to disk.

    Streaming rather than holding it in memory: the VBB export is a few
    hundred megabytes and this container is capped well below that.

    Args:
        url: Archive URL.
        target: Destination file.
        timeout: Per request timeout in seconds.

    Raises:
        RuntimeError: The download failed.
    """
    _log.info("downloading GTFS archive from %s", url)
    with requests.get(url, stream=True, timeout=timeout) as response:
        if response.status_code != 200:
            raise RuntimeError(f"GTFS download returned HTTP {response.status_code}")
        written = 0
        with target.open("wb") as handle:
            for chunk in response.iter_content(CHUNK_BYTES):
                handle.write(chunk)
                written += len(chunk)
    _log.info("downloaded %d bytes", written)


def read_table(archive: zipfile.ZipFile, name: str) -> Iterator[dict[str, str]]:
    """Yield rows of one CSV member of the archive.

    Args:
        archive: Open GTFS zip archive.
        name: Member name, for example "stops.txt".

    Yields:
        One dict per row, keyed by column name.

    Raises:
        KeyError: The member is missing from the archive.
    """
    with archive.open(name) as raw:
        # GTFS exports are UTF-8, often with a byte order mark.
        text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
        yield from csv.DictReader(text)


def load_stops(
    connection: psycopg.Connection, archive: zipfile.ZipFile, prefix: str
) -> int:
    """Upsert stops.txt into gtfs_stops.

    Args:
        connection: Open database connection.
        archive: GTFS archive.
        prefix: Keep only stop ids with this prefix. Empty keeps all.

    Returns:
        Number of rows written.
    """
    statement = (
        "INSERT INTO gtfs_stops (stop_id, stop_name, parent_station, stop_lat, stop_lon, loaded_at) "
        "VALUES (%s, %s, %s, %s, %s, now()) "
        "ON CONFLICT (stop_id) DO UPDATE SET "
        "stop_name = EXCLUDED.stop_name, parent_station = EXCLUDED.parent_station, "
        "stop_lat = EXCLUDED.stop_lat, stop_lon = EXCLUDED.stop_lon, loaded_at = now()"
    )
    batch: list[tuple[str, str, str | None, float | None, float | None]] = []
    written = 0
    with connection.cursor() as cursor:
        for row in read_table(archive, "stops.txt"):
            stop_id = row["stop_id"]
            if prefix and not stop_id.startswith(prefix):
                continue
            batch.append(
                (
                    stop_id,
                    row.get("stop_name") or stop_id,
                    row.get("parent_station") or None,
                    _float(row.get("stop_lat")),
                    _float(row.get("stop_lon")),
                )
            )
            if len(batch) >= BATCH_ROWS:
                cursor.executemany(statement, batch)
                written += len(batch)
                batch.clear()
        if batch:
            cursor.executemany(statement, batch)
            written += len(batch)
    connection.commit()
    return written


def load_routes(connection: psycopg.Connection, archive: zipfile.ZipFile) -> int:
    """Upsert routes.txt into gtfs_routes.

    Args:
        connection: Open database connection.
        archive: GTFS archive.

    Returns:
        Number of rows written.
    """
    statement = (
        "INSERT INTO gtfs_routes (route_id, route_short_name, route_long_name, route_type, loaded_at) "
        "VALUES (%s, %s, %s, %s, now()) "
        "ON CONFLICT (route_id) DO UPDATE SET "
        "route_short_name = EXCLUDED.route_short_name, "
        "route_long_name = EXCLUDED.route_long_name, "
        "route_type = EXCLUDED.route_type, loaded_at = now()"
    )
    batch: list[tuple[str, str, str | None, int | None]] = []
    written = 0
    with connection.cursor() as cursor:
        for row in read_table(archive, "routes.txt"):
            route_id = row["route_id"]
            batch.append(
                (
                    route_id,
                    row.get("route_short_name") or route_id,
                    row.get("route_long_name") or None,
                    _int(row.get("route_type")),
                )
            )
            if len(batch) >= BATCH_ROWS:
                cursor.executemany(statement, batch)
                written += len(batch)
                batch.clear()
        if batch:
            cursor.executemany(statement, batch)
            written += len(batch)
    connection.commit()
    return written


def _float(value: str | None) -> float | None:
    """Parse a coordinate, tolerating blanks the export leaves behind."""
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _int(value: str | None) -> int | None:
    """Parse an integer column, tolerating blanks."""
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def main() -> int:
    """Download and load the dimension tables.

    Returns:
        Process exit code.
    """
    url = os.environ.get("GTFS_URL", DEFAULT_GTFS_URL)
    dsn = os.environ.get("PG_DSN", "")
    if not dsn:
        _log.error("PG_DSN is required")
        return 2
    prefix = os.environ.get("STOP_ID_PREFIX", "de:11000:")
    timeout = int(os.environ.get("HTTP_TIMEOUT_SECONDS", "300"))

    with tempfile.TemporaryDirectory() as workdir:
        archive_path = Path(workdir) / "gtfs.zip"
        try:
            download(url, archive_path, timeout)
        except (requests.RequestException, RuntimeError) as exc:
            _log.error("GTFS download failed: %s", exc)
            return 1

        try:
            with zipfile.ZipFile(archive_path) as archive, psycopg.connect(dsn) as connection:
                stops = load_stops(connection, archive, prefix)
                routes = load_routes(connection, archive)
        except zipfile.BadZipFile as exc:
            _log.error("GTFS archive is not readable: %s", exc)
            return 1
        except psycopg.Error as exc:
            _log.error("database error while loading dimensions: %s", exc)
            return 1

    _log.info("loaded %d stops and %d routes", stops, routes)
    if stops == 0:
        _log.error("no stops matched prefix %r; check STOP_ID_PREFIX", prefix)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
