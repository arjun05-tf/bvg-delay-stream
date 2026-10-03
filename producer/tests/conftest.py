"""Shared fixtures.

The fixtures build FeedMessage objects that mirror the shape of a real VBB
snapshot captured on 2026-10-02, including its warts: stop time updates with
no stop_sequence sitting next to SKIPPED stops, stop times with a time but no
delay, and trips whose stops are still in the future.
"""

from __future__ import annotations

import pytest
from google.transit import gtfs_realtime_pb2

FEED_TIMESTAMP = 1_790_977_022  # 2026-10-02T21:37:02Z


def _add_stop(
    trip_update: gtfs_realtime_pb2.TripUpdate,
    *,
    stop_id: str,
    stop_sequence: int | None,
    arrival_time: int | None = None,
    arrival_delay: int | None = None,
    departure_time: int | None = None,
    departure_delay: int | None = None,
    schedule_relationship: int = 0,
) -> None:
    """Append one stop time update with exactly the fields given."""
    update = trip_update.stop_time_update.add()
    update.stop_id = stop_id
    if stop_sequence is not None:
        update.stop_sequence = stop_sequence
    update.schedule_relationship = schedule_relationship
    if arrival_time is not None:
        update.arrival.time = arrival_time
        if arrival_delay is not None:
            update.arrival.delay = arrival_delay
    if departure_time is not None:
        update.departure.time = departure_time
        if departure_delay is not None:
            update.departure.delay = departure_delay


@pytest.fixture
def feed() -> gtfs_realtime_pb2.FeedMessage:
    """A snapshot with one Berlin trip, one Brandenburg trip and one shadow row."""
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    message.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    message.header.timestamp = FEED_TIMESTAMP

    berlin = message.entity.add()
    berlin.id = "3644897851.9041233"
    berlin.trip_update.trip.trip_id = "307524656"
    berlin.trip_update.trip.start_date = "20261002"
    berlin.trip_update.trip.route_id = "17330_700"
    berlin.trip_update.trip.direction_id = 0
    berlin.trip_update.timestamp = FEED_TIMESTAMP - 4879
    # Past stop with a delay on both arrival and departure.
    _add_stop(
        berlin.trip_update,
        stop_id="de:11000:900100516::1",
        stop_sequence=0,
        arrival_time=FEED_TIMESTAMP - 600,
        arrival_delay=180,
        departure_time=FEED_TIMESTAMP - 540,
        departure_delay=120,
    )
    # Past stop with times but no delay at all.
    _add_stop(
        berlin.trip_update,
        stop_id="de:11000:900100008::4",
        stop_sequence=1,
        arrival_time=FEED_TIMESTAMP - 300,
        departure_time=FEED_TIMESTAMP - 240,
    )
    # Future prediction. Must not be emitted.
    _add_stop(
        berlin.trip_update,
        stop_id="de:11000:900100521::1",
        stop_sequence=2,
        arrival_time=FEED_TIMESTAMP + 600,
        arrival_delay=60,
    )
    # Too old for the default one hour horizon.
    _add_stop(
        berlin.trip_update,
        stop_id="de:11000:900014107::4",
        stop_sequence=3,
        arrival_time=FEED_TIMESTAMP - 7200,
        arrival_delay=0,
    )

    brandenburg = message.entity.add()
    brandenburg.id = "3644961709.606791"
    brandenburg.trip_update.trip.trip_id = "306895949"
    brandenburg.trip_update.trip.start_date = "20261002"
    brandenburg.trip_update.trip.route_id = "27280_700"
    brandenburg.trip_update.trip.direction_id = 1
    _add_stop(
        brandenburg.trip_update,
        stop_id="de:12067:900310558::2",
        stop_sequence=17,
        arrival_time=FEED_TIMESTAMP - 120,
        arrival_delay=300,
        schedule_relationship=1,
    )
    # The shadow row the feed emits next to a SKIPPED stop: no stop_sequence,
    # raw non-DHID stop id. Must go to the dead letter topic.
    _add_stop(
        brandenburg.trip_update,
        stop_id="110558",
        stop_sequence=None,
        arrival_time=FEED_TIMESTAMP - 120,
    )
    return message
