"""Consumer lag probe.

Samples the lag of the Flink consumer group and writes it to TimescaleDB,
which is what the load test reads to answer "what degraded first". Writing to
the same database the dashboard already reads keeps Prometheus out of the
compose file, which is worth roughly 300 MB of RAM on an 8 GB machine.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
from types import FrameType

import psycopg
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
)
_log = logging.getLogger("lag_probe")

_running = True


def _stop(signum: int, _frame: FrameType | None) -> None:
    """Request shutdown at the next sample boundary."""
    global _running
    _running = False
    _log.info("stopping on signal %s", signum)


def partitions_of(admin: AdminClient, topic: str, timeout: float = 10.0) -> list[int]:
    """Return the partition ids of a topic.

    Args:
        admin: Kafka admin client.
        topic: Topic name.
        timeout: Metadata request timeout in seconds.

    Returns:
        Sorted partition ids. Empty when the topic does not exist yet.
    """
    metadata = admin.list_topics(topic=topic, timeout=timeout)
    if topic not in metadata.topics or metadata.topics[topic].error is not None:
        return []
    return sorted(metadata.topics[topic].partitions)


def sample_lag(
    consumer: Consumer, topic: str, partition_ids: list[int]
) -> list[tuple[int, int, int]]:
    """Measure lag per partition.

    Args:
        consumer: Consumer bound to the group under observation. It must not
            subscribe: committed offsets are queried directly so the probe
            never joins the group and never steals partitions from the job.
        topic: Topic name.
        partition_ids: Partitions to measure.

    Returns:
        Tuples of (partition, lag, high_watermark).
    """
    requested = [TopicPartition(topic, partition) for partition in partition_ids]
    committed = consumer.committed(requested, timeout=10)
    samples: list[tuple[int, int, int]] = []
    for entry in committed:
        _low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, entry.partition), timeout=10, cached=False
        )
        # A group that has never committed reports a negative offset. Treat
        # the whole partition as outstanding, which is what it is.
        position = entry.offset if entry.offset is not None and entry.offset >= 0 else 0
        samples.append((entry.partition, max(0, high - position), high))
    return samples


def main() -> int:
    """Sample lag on an interval until stopped.

    Returns:
        Process exit code.
    """
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
    dsn = os.environ.get("PG_DSN", "")
    if not bootstrap or not dsn:
        _log.error("KAFKA_BOOTSTRAP_SERVERS and PG_DSN are required")
        return 2

    topic = os.environ.get("TOPIC_STOP_EVENTS", "stop-events")
    group = os.environ.get("CONSUMER_GROUP", "delay-pipeline")
    interval = float(os.environ.get("LAG_SAMPLE_INTERVAL_S", "10"))

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    admin = AdminClient({"bootstrap.servers": bootstrap})
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
            # The probe must never take partitions away from the job.
            "group.instance.id": "lag-probe",
        }
    )

    _log.info("probing lag for group=%s topic=%s every %.0fs", group, topic, interval)
    exit_code = 0
    try:
        with psycopg.connect(dsn, autocommit=True) as connection:
            while _running:
                started = time.monotonic()
                try:
                    partition_ids = partitions_of(admin, topic)
                    if not partition_ids:
                        _log.warning("topic %s has no partitions yet", topic)
                    else:
                        samples = sample_lag(consumer, topic, partition_ids)
                        with connection.cursor() as cursor:
                            cursor.executemany(
                                "INSERT INTO consumer_lag_samples "
                                "(sampled_at, consumer_group, topic, partition, lag, high_watermark) "
                                "VALUES (now(), %s, %s, %s, %s, %s)",
                                [
                                    (group, topic, partition, lag, high)
                                    for partition, lag, high in samples
                                ],
                            )
                        _log.info(
                            "lag sampled total=%d partitions=%d",
                            sum(lag for _p, lag, _h in samples),
                            len(samples),
                        )
                except Exception as exc:  # noqa: BLE001 - probe must outlive any single failure
                    _log.warning("lag sample failed: %s", exc)

                remaining = interval - (time.monotonic() - started)
                while remaining > 0 and _running:
                    step = min(0.5, remaining)
                    time.sleep(step)
                    remaining -= step
    except psycopg.Error as exc:
        _log.error("database unavailable: %s", exc)
        exit_code = 1
    finally:
        consumer.close()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
