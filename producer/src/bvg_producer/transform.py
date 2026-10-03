"""Turn a GTFS-Realtime FeedMessage into StopEvent records.

Three decisions live in this module and nowhere else.

1. Event time is the stop event's own reported time, not the entity
   timestamp. Entity timestamps inside one VBB snapshot span several hours,
   so using them as event time forces a watermark lag of hours and makes
   every window useless.

2. Only stop events at or before the feed header timestamp are emitted. The
   feed is mostly predictions for stops that have not happened yet; their
   times run ahead of wall clock and would drag watermarks into the future.
   A prediction becomes an observation once the snapshot clock passes it.

3. ``delay`` is optional in the feed and zero is a real value, so absence is
   carried explicitly in ``has_delay`` rather than encoded as 0.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from google.transit import gtfs_realtime_pb2

from .generated.delay.v1 import stop_event_pb2

# Stop time updates that arrive without a stop_sequence are the malformed
# shadow entries the VBB feed emits next to SKIPPED stops. They carry a raw,
# non-DHID stop id and cannot be joined to anything.
REASON_NO_STOP_SEQUENCE = "missing_stop_sequence"
REASON_NO_STOP_ID = "missing_stop_id"
REASON_NO_TRIP_ID = "missing_trip_id"
REASON_NO_TIMES = "no_arrival_or_departure_time"


@dataclass(frozen=True, slots=True)
class RejectedRecord:
    """A stop time update that could not be turned into a StopEvent."""

    reason: str
    entity_id: str
    detail: str


@dataclass(slots=True)
class TransformStats:
    """Counters for one snapshot. Every stop time update lands in exactly one."""

    stop_time_updates: int = 0
    emitted: int = 0
    future_skipped: int = 0
    too_old_skipped: int = 0
    prefix_filtered: int = 0
    rejected: int = 0
    with_delay: int = 0


@dataclass(slots=True)
class TransformResult:
    """Output of one snapshot transform."""

    events: list[stop_event_pb2.StopEvent] = field(default_factory=list)
    rejected: list[RejectedRecord] = field(default_factory=list)
    stats: TransformStats = field(default_factory=TransformStats)


def _direction_id(trip: gtfs_realtime_pb2.TripDescriptor) -> int:
    """Return the trip direction, or -1 when the feed omitted it."""
    return trip.direction_id if trip.HasField("direction_id") else -1


def transform_feed(
    feed: gtfs_realtime_pb2.FeedMessage,
    *,
    stop_id_prefix: str = "",
    max_event_age_seconds: int = 3600,
    ingest_timestamp: int | None = None,
) -> TransformResult:
    """Extract observed stop events from one feed snapshot.

    Args:
        feed: Parsed feed snapshot.
        stop_id_prefix: Keep only stops whose id starts with this prefix.
            ``"de:11000:"`` restricts the stream to Berlin. Empty keeps the
            whole VBB area.
        max_event_age_seconds: Drop observations older than this relative to
            the feed header timestamp. Without this bound, a snapshot that
            restates stops from early morning would widen every window.
        ingest_timestamp: Unix seconds stamped onto each event. Defaults to
            now. Injected in tests to keep output deterministic.

    Returns:
        A TransformResult holding the events, the rejected records and the
        counters that explain what happened to everything else.
    """
    feed_timestamp = int(feed.header.timestamp)
    ingest = int(time.time()) if ingest_timestamp is None else ingest_timestamp
    oldest_accepted = feed_timestamp - max_event_age_seconds
    result = TransformResult()

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        trip_update = entity.trip_update
        trip = trip_update.trip

        if not trip.trip_id:
            result.stats.rejected += 1
            result.rejected.append(
                RejectedRecord(REASON_NO_TRIP_ID, entity.id, "trip descriptor has no trip_id")
            )
            continue

        for update in trip_update.stop_time_update:
            result.stats.stop_time_updates += 1

            if not update.stop_id:
                result.stats.rejected += 1
                result.rejected.append(
                    RejectedRecord(REASON_NO_STOP_ID, entity.id, f"trip {trip.trip_id}")
                )
                continue

            if not update.HasField("stop_sequence"):
                result.stats.rejected += 1
                result.rejected.append(
                    RejectedRecord(
                        REASON_NO_STOP_SEQUENCE,
                        entity.id,
                        f"trip {trip.trip_id} stop {update.stop_id}",
                    )
                )
                continue

            if stop_id_prefix and not update.stop_id.startswith(stop_id_prefix):
                result.stats.prefix_filtered += 1
                continue

            pairs = (
                (stop_event_pb2.StopEvent.EVENT_TYPE_ARRIVAL, update.arrival
                 if update.HasField("arrival") else None),
                (stop_event_pb2.StopEvent.EVENT_TYPE_DEPARTURE, update.departure
                 if update.HasField("departure") else None),
            )

            produced_any = False
            for event_type, stop_time in pairs:
                if stop_time is None or not stop_time.HasField("time"):
                    continue
                produced_any = True
                observed = int(stop_time.time)

                if observed > feed_timestamp:
                    result.stats.future_skipped += 1
                    continue
                if observed < oldest_accepted:
                    result.stats.too_old_skipped += 1
                    continue

                has_delay = stop_time.HasField("delay")
                if has_delay:
                    result.stats.with_delay += 1

                result.events.append(
                    stop_event_pb2.StopEvent(
                        trip_id=trip.trip_id,
                        start_date=trip.start_date,
                        route_id=trip.route_id,
                        direction_id=_direction_id(trip),
                        stop_id=update.stop_id,
                        stop_sequence=update.stop_sequence,
                        event_type=event_type,
                        observed_time=observed,
                        delay_seconds=int(stop_time.delay) if has_delay else 0,
                        has_delay=has_delay,
                        schedule_relationship=int(update.schedule_relationship),
                        feed_timestamp=feed_timestamp,
                        ingest_timestamp=ingest,
                        feed_entity_id=entity.id,
                    )
                )
                result.stats.emitted += 1

            if not produced_any:
                result.stats.rejected += 1
                result.rejected.append(
                    RejectedRecord(
                        REASON_NO_TIMES,
                        entity.id,
                        f"trip {trip.trip_id} stop {update.stop_id}",
                    )
                )

    return result


def event_key(event: stop_event_pb2.StopEvent) -> bytes:
    """Return the Kafka message key for a StopEvent.

    Keyed by trip instance, so every event of one trip lands in one
    partition and keeps its order. The stop itself is deliberately not in the
    key: per-trip ordering is what the windowing and the replay proof need.
    """
    return f"{event.trip_id}|{event.start_date}".encode()
