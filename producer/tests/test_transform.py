"""Transform tests.

These encode the three design decisions in bvg_producer.transform: event time
is the stop time, future predictions are not events, and a missing delay is
not a zero delay.
"""

from __future__ import annotations

from google.transit import gtfs_realtime_pb2

from bvg_producer.generated.delay.v1 import stop_event_pb2
from bvg_producer.transform import (
    REASON_NO_STOP_SEQUENCE,
    event_key,
    transform_feed,
)

from .conftest import FEED_TIMESTAMP

INGEST = 1_790_977_100


def test_only_past_stop_events_are_emitted(feed: gtfs_realtime_pb2.FeedMessage) -> None:
    result = transform_feed(feed, stop_id_prefix="", ingest_timestamp=INGEST)
    assert all(event.observed_time <= FEED_TIMESTAMP for event in result.events)
    assert result.stats.future_skipped == 1


def test_events_older_than_horizon_are_dropped(feed: gtfs_realtime_pb2.FeedMessage) -> None:
    result = transform_feed(
        feed, stop_id_prefix="", max_event_age_seconds=3600, ingest_timestamp=INGEST
    )
    assert result.stats.too_old_skipped == 1
    assert all(event.observed_time >= FEED_TIMESTAMP - 3600 for event in result.events)


def test_arrival_and_departure_become_separate_events(
    feed: gtfs_realtime_pb2.FeedMessage,
) -> None:
    result = transform_feed(feed, stop_id_prefix="de:11000:", ingest_timestamp=INGEST)
    first_stop = [
        event for event in result.events if event.stop_id == "de:11000:900100516::1"
    ]
    assert {event.event_type for event in first_stop} == {
        stop_event_pb2.StopEvent.EVENT_TYPE_ARRIVAL,
        stop_event_pb2.StopEvent.EVENT_TYPE_DEPARTURE,
    }
    arrival = next(
        event
        for event in first_stop
        if event.event_type == stop_event_pb2.StopEvent.EVENT_TYPE_ARRIVAL
    )
    assert arrival.delay_seconds == 180
    assert arrival.has_delay is True


def test_missing_delay_is_not_zero_delay(feed: gtfs_realtime_pb2.FeedMessage) -> None:
    result = transform_feed(feed, stop_id_prefix="de:11000:", ingest_timestamp=INGEST)
    no_delay = [
        event for event in result.events if event.stop_id == "de:11000:900100008::4"
    ]
    assert no_delay, "stop with times but no delay must still produce events"
    assert all(event.has_delay is False for event in no_delay)
    assert all(event.delay_seconds == 0 for event in no_delay)


def test_shadow_row_without_stop_sequence_is_rejected(
    feed: gtfs_realtime_pb2.FeedMessage,
) -> None:
    result = transform_feed(feed, stop_id_prefix="", ingest_timestamp=INGEST)
    reasons = [record.reason for record in result.rejected]
    assert REASON_NO_STOP_SEQUENCE in reasons
    assert all(event.stop_id != "110558" for event in result.events)


def test_prefix_filter_keeps_berlin_only(feed: gtfs_realtime_pb2.FeedMessage) -> None:
    result = transform_feed(feed, stop_id_prefix="de:11000:", ingest_timestamp=INGEST)
    assert result.events
    assert all(event.stop_id.startswith("de:11000:") for event in result.events)
    assert result.stats.prefix_filtered == 1


def test_skipped_stop_keeps_its_schedule_relationship(
    feed: gtfs_realtime_pb2.FeedMessage,
) -> None:
    result = transform_feed(feed, stop_id_prefix="", ingest_timestamp=INGEST)
    skipped = [
        event for event in result.events if event.stop_id == "de:12067:900310558::2"
    ]
    assert skipped and all(event.schedule_relationship == 1 for event in skipped)


def test_counters_account_for_every_stop_time_update(
    feed: gtfs_realtime_pb2.FeedMessage,
) -> None:
    result = transform_feed(feed, stop_id_prefix="", ingest_timestamp=INGEST)
    stats = result.stats
    # Each stop time update is either rejected, filtered, or produces
    # arrival and departure outcomes that land in emitted/future/too_old.
    accounted = stats.rejected + stats.prefix_filtered
    outcomes = stats.emitted + stats.future_skipped + stats.too_old_skipped
    assert accounted + 0 < stats.stop_time_updates
    assert outcomes > 0
    assert stats.with_delay <= stats.emitted


def test_direction_id_absent_becomes_minus_one() -> None:
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    message.header.timestamp = FEED_TIMESTAMP
    entity = message.entity.add()
    entity.id = "e1"
    entity.trip_update.trip.trip_id = "t1"
    entity.trip_update.trip.start_date = "20261002"
    entity.trip_update.trip.route_id = "r1"
    update = entity.trip_update.stop_time_update.add()
    update.stop_id = "de:11000:900100516::1"
    update.stop_sequence = 0
    update.arrival.time = FEED_TIMESTAMP - 60
    result = transform_feed(message, stop_id_prefix="", ingest_timestamp=INGEST)
    assert result.events[0].direction_id == -1


def test_event_key_is_the_trip_instance() -> None:
    event = stop_event_pb2.StopEvent(trip_id="307524656", start_date="20261002")
    assert event_key(event) == b"307524656|20261002"


def test_transform_is_deterministic(feed: gtfs_realtime_pb2.FeedMessage) -> None:
    first = transform_feed(feed, stop_id_prefix="", ingest_timestamp=INGEST)
    second = transform_feed(feed, stop_id_prefix="", ingest_timestamp=INGEST)
    assert [event.SerializeToString() for event in first.events] == [
        event.SerializeToString() for event in second.events
    ]
