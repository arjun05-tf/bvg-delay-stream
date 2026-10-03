"""Prometheus metrics.

Exposed on an HTTP port so the load test can scrape them, and so the numbers
in the README headline table come from instrumentation rather than from
reading log lines.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

FEED_FETCHES = Counter(
    "bvg_feed_fetches_total", "Feed fetch attempts by outcome.", ["outcome"]
)
FEED_FETCH_SECONDS = Histogram(
    "bvg_feed_fetch_seconds",
    "Wall time of one feed fetch.",
    buckets=(0.25, 0.5, 1, 2, 4, 8, 16, 32),
)
FEED_PAYLOAD_BYTES = Gauge(
    "bvg_feed_payload_bytes", "Size of the most recent feed snapshot."
)
FEED_AGE_SECONDS = Gauge(
    "bvg_feed_age_seconds",
    "Wall clock now minus the feed header timestamp of the last snapshot.",
)
STOP_TIME_UPDATES = Counter(
    "bvg_stop_time_updates_total",
    "Stop time updates seen, by what the transform did with them.",
    ["disposition"],
)
EVENTS_PUBLISHED = Counter(
    "bvg_events_published_total", "StopEvent messages handed to the Kafka producer."
)
EVENTS_DELIVERED = Counter(
    "bvg_events_delivered_total", "StopEvent messages acknowledged by the broker."
)
DELIVERY_FAILURES = Counter(
    "bvg_delivery_failures_total", "Kafka deliveries that failed permanently."
)
DLQ_MESSAGES = Counter(
    "bvg_dlq_messages_total", "Messages written to the dead letter topic.", ["reason"]
)
DELAY_COVERAGE = Gauge(
    "bvg_delay_coverage_ratio",
    "Share of emitted events in the last snapshot that carried a delay value.",
)
POLL_ERRORS = Counter(
    "bvg_poll_errors_total", "Polling errors by class.", ["kind"]
)


def serve_metrics(port: int) -> None:
    """Start the metrics HTTP server on all interfaces.

    Args:
        port: TCP port to listen on.
    """
    start_http_server(port)
