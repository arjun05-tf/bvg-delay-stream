"""Producer entry point.

Polls the VBB GTFS-Realtime feed on a fixed interval, turns each snapshot
into StopEvent messages and publishes them to Kafka. Transient failures back
off exponentially; fatal failures exit non-zero so the container restarts
under a supervisor that can show the operator the reason.
"""

from __future__ import annotations

import logging
import random
import signal
import sys
import time
from types import FrameType

from . import metrics
from .config import Config
from .decode import parse_feed
from .errors import FatalError, MalformedFeedError, RetryableError
from .feed_client import FeedClient
from .logging_setup import configure_logging
from .publisher import EventPublisher
from .recorder import SnapshotRecorder
from .registry import build_serializer
from .transform import transform_feed

_log = logging.getLogger("bvg_producer")

_shutdown = False


def _handle_signal(signum: int, _frame: FrameType | None) -> None:
    """Request a clean shutdown at the next loop boundary."""
    global _shutdown
    _shutdown = True
    _log.info("shutdown requested", extra={"signal": signum})


def _record_stats(result_stats: object) -> None:
    """Publish transform counters to Prometheus."""
    stats = result_stats
    for disposition in (
        "emitted",
        "future_skipped",
        "too_old_skipped",
        "prefix_filtered",
        "rejected",
    ):
        metrics.STOP_TIME_UPDATES.labels(disposition=disposition).inc(
            getattr(stats, disposition)
        )


def run(config: Config) -> int:
    """Run the polling loop until a signal arrives or a fatal error occurs.

    Args:
        config: Validated configuration.

    Returns:
        Process exit code. 0 on clean shutdown, 1 on fatal error.
    """
    metrics.serve_metrics(config.metrics_port)
    client = FeedClient(config.feed_url, config.http_timeout_seconds, config.user_agent)
    serializer = build_serializer(config.schema_registry_url)
    publisher = EventPublisher(
        config.bootstrap_servers,
        serializer,
        config.topic_stop_events,
        config.topic_dlq,
    )
    recorder = (
        SnapshotRecorder(config.record_dir, config.record_retention)
        if config.record_dir
        else None
    )

    backoff = config.backoff_initial_seconds
    exit_code = 0

    _log.info(
        "producer started",
        extra={
            "feed_url": config.feed_url,
            "topic": config.topic_stop_events,
            "poll_interval_s": config.poll_interval_seconds,
            "stop_id_prefix": config.stop_id_prefix or "(none)",
        },
    )

    while not _shutdown:
        cycle_started = time.monotonic()
        try:
            snapshot = client.fetch()
        except RetryableError as exc:
            metrics.FEED_FETCHES.labels(outcome="retryable_error").inc()
            metrics.POLL_ERRORS.labels(kind="retryable").inc()
            sleep_for = min(backoff, config.backoff_max_seconds) * (0.5 + random.random())
            _log.warning(
                "feed fetch failed, backing off",
                extra={"error": str(exc), "sleep_s": round(sleep_for, 2)},
            )
            time.sleep(sleep_for)
            backoff = min(backoff * 2, config.backoff_max_seconds)
            continue
        except FatalError as exc:
            metrics.FEED_FETCHES.labels(outcome="fatal_error").inc()
            metrics.POLL_ERRORS.labels(kind="fatal").inc()
            _log.critical("feed fetch failed permanently", extra={"error": str(exc)})
            exit_code = 1
            break

        backoff = config.backoff_initial_seconds

        if snapshot is None:
            metrics.FEED_FETCHES.labels(outcome="not_modified").inc()
        else:
            metrics.FEED_FETCHES.labels(outcome="ok").inc()
            metrics.FEED_FETCH_SECONDS.observe(snapshot.fetch_duration_seconds)
            metrics.FEED_PAYLOAD_BYTES.set(len(snapshot.payload))
            try:
                feed = parse_feed(snapshot.payload)
            except MalformedFeedError as exc:
                publisher.publish_dead_letter("malformed_feed", exc.payload, str(exc))
                _log.error("snapshot could not be parsed", extra={"error": str(exc)})
            else:
                metrics.FEED_AGE_SECONDS.set(time.time() - feed.header.timestamp)
                if recorder is not None:
                    recorder.record(snapshot.payload, int(feed.header.timestamp))

                result = transform_feed(
                    feed,
                    stop_id_prefix=config.stop_id_prefix,
                    max_event_age_seconds=config.max_event_age_seconds,
                )
                _record_stats(result.stats)
                if result.stats.emitted:
                    metrics.DELAY_COVERAGE.set(
                        result.stats.with_delay / result.stats.emitted
                    )

                for rejected in result.rejected:
                    publisher.publish_dead_letter(
                        rejected.reason,
                        rejected.detail.encode(),
                        f"entity={rejected.entity_id}",
                    )

                for event in result.events:
                    while True:
                        try:
                            publisher.publish(event)
                        except RetryableError:
                            # Local queue full: drain callbacks, then retry
                            # the same event. Dropping it would silently lose
                            # data the replay proof claims is lossless.
                            publisher.poll(0.5)
                            continue
                        break

                publisher.poll(0)
                _log.info(
                    "snapshot processed",
                    extra={
                        "feed_timestamp": int(feed.header.timestamp),
                        "entities": len(feed.entity),
                        "stop_time_updates": result.stats.stop_time_updates,
                        "emitted": result.stats.emitted,
                        "with_delay": result.stats.with_delay,
                        "rejected": result.stats.rejected,
                        "future_skipped": result.stats.future_skipped,
                        "payload_bytes": len(snapshot.payload),
                    },
                )

        elapsed = time.monotonic() - cycle_started
        remaining = config.poll_interval_seconds - elapsed
        while remaining > 0 and not _shutdown:
            step = min(1.0, remaining)
            publisher.poll(step)
            remaining -= step

    pending = publisher.flush(30.0)
    if pending:
        _log.error("shutdown with undelivered messages", extra={"pending": pending})
        exit_code = 1
    client.close()
    _log.info("producer stopped", extra={"exit_code": exit_code})
    return exit_code


def main() -> int:
    """Parse configuration, install signal handlers and run.

    Returns:
        Process exit code.
    """
    try:
        config = Config.from_env()
    except FatalError as exc:
        configure_logging("INFO")
        logging.getLogger("bvg_producer").critical("startup failed", extra={"error": str(exc)})
        return 2

    configure_logging(config.log_level)
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    try:
        return run(config)
    except FatalError as exc:
        _log.critical("fatal error", extra={"error": str(exc)})
        return 1


if __name__ == "__main__":
    sys.exit(main())
