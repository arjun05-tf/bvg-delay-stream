"""Load generator.

Replays recorded feed snapshots into Kafka at a multiple of their natural
rate. The point of using recorded snapshots rather than synthetic messages is
that the key distribution, the message sizes and the delay distribution are
the real ones, so the load test stresses the same code paths as production
traffic rather than a tidy approximation of it.

Environment:
    SNAPSHOT_DIR               directory of .pb files written by the producer
    SPEEDUP                    rate multiplier, 10 means ten times natural
    NATURAL_INTERVAL_SECONDS   seconds between snapshots in the recording
    LOOPS                      how many times to walk the corpus, 0 is forever
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
from pathlib import Path
from types import FrameType

from bvg_producer.config import Config
from bvg_producer.decode import parse_feed
from bvg_producer.errors import MalformedFeedError, RetryableError
from bvg_producer.logging_setup import configure_logging
from bvg_producer.publisher import EventPublisher
from bvg_producer.registry import build_serializer
from bvg_producer.transform import transform_feed

_log = logging.getLogger("loadgen")
_running = True


def _stop(signum: int, _frame: FrameType | None) -> None:
    """Stop at the next snapshot boundary."""
    global _running
    _running = False
    _log.info("stopping", extra={"signal": signum})


def snapshots(directory: Path) -> list[Path]:
    """Return recorded snapshots in chronological order.

    Args:
        directory: Directory of .pb files named after their feed timestamp.

    Returns:
        Sorted list of snapshot paths.
    """
    return sorted(directory.glob("*.pb"))


def main() -> int:
    """Replay the corpus at the configured speedup.

    Returns:
        Process exit code.
    """
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    directory = Path(os.environ.get("SNAPSHOT_DIR", "/snapshots"))
    speedup = float(os.environ.get("SPEEDUP", "10"))
    natural_interval = float(os.environ.get("NATURAL_INTERVAL_SECONDS", "30"))
    loops = int(os.environ.get("LOOPS", "1"))

    if speedup <= 0:
        _log.error("SPEEDUP must be positive")
        return 2

    files = snapshots(directory)
    if not files:
        _log.error("no snapshots found", extra={"directory": str(directory)})
        return 2

    config = Config.from_env()
    publisher = EventPublisher(
        config.bootstrap_servers,
        build_serializer(config.schema_registry_url),
        config.topic_stop_events,
        config.topic_dlq,
    )

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    interval = natural_interval / speedup
    _log.info(
        "replay starting",
        extra={
            "snapshots": len(files),
            "speedup": speedup,
            "interval_s": round(interval, 3),
            "loops": loops or "forever",
        },
    )

    published = 0
    started = time.monotonic()
    last_report = started
    loop = 0

    while _running and (loops == 0 or loop < loops):
        loop += 1
        for path in files:
            if not _running:
                break
            cycle_started = time.monotonic()
            try:
                feed = parse_feed(path.read_bytes())
            except MalformedFeedError as exc:
                _log.warning("skipping unreadable snapshot", extra={"file": path.name, "error": str(exc)})
                continue

            result = transform_feed(
                feed,
                stop_id_prefix=config.stop_id_prefix,
                max_event_age_seconds=config.max_event_age_seconds,
            )
            for event in result.events:
                while True:
                    try:
                        publisher.publish(event)
                    except RetryableError:
                        publisher.poll(0.2)
                        continue
                    break
                published += 1
            publisher.poll(0)

            now = time.monotonic()
            if now - last_report >= 10:
                elapsed = now - started
                _log.info(
                    "load progress",
                    extra={
                        "published": published,
                        "elapsed_s": round(elapsed, 1),
                        "messages_per_second": round(published / elapsed, 1),
                    },
                )
                last_report = now

            remaining = interval - (now - cycle_started)
            if remaining > 0:
                # Poll rather than sleep, so delivery callbacks keep draining.
                publisher.poll(remaining)
            else:
                _log.warning(
                    "cannot keep up with the requested rate",
                    extra={"behind_s": round(-remaining, 3), "snapshot": path.name},
                )

    pending = publisher.flush(60.0)
    elapsed = max(time.monotonic() - started, 1e-6)
    _log.info(
        "load finished",
        extra={
            "published": published,
            "undelivered": pending,
            "elapsed_s": round(elapsed, 1),
            "messages_per_second": round(published / elapsed, 1),
        },
    )
    return 1 if pending else 0


if __name__ == "__main__":
    sys.exit(main())
