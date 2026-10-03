"""Environment driven configuration with eager validation.

Configuration is read once at startup and every problem is reported in a
single exception. A service that dies on the first bad variable makes the
operator restart it five times to find five typos.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .errors import ConfigError

DEFAULT_FEED_URL = "https://production.gtfsrt.vbb.de/data"


def _get_int(env: dict[str, str], name: str, default: int, problems: list[str],
             minimum: int | None = None, maximum: int | None = None) -> int:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        problems.append(f"{name}: {raw!r} is not an integer")
        return default
    if minimum is not None and value < minimum:
        problems.append(f"{name}: {value} is below the minimum {minimum}")
    if maximum is not None and value > maximum:
        problems.append(f"{name}: {value} is above the maximum {maximum}")
    return value


def _get_required(env: dict[str, str], name: str, problems: list[str]) -> str:
    value = env.get(name, "").strip()
    if not value:
        problems.append(f"{name}: required but not set")
    return value


@dataclass(frozen=True, slots=True)
class Config:
    """Validated producer configuration."""

    feed_url: str
    poll_interval_seconds: int
    http_timeout_seconds: int
    user_agent: str

    bootstrap_servers: str
    schema_registry_url: str
    topic_stop_events: str
    topic_dlq: str

    stop_id_prefix: str
    max_event_age_seconds: int

    record_dir: Path | None
    record_retention: int

    metrics_port: int
    log_level: str

    # Backoff bounds for retryable feed errors.
    backoff_initial_seconds: float = 1.0
    backoff_max_seconds: float = 60.0

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Config:
        """Build a Config from environment variables.

        Args:
            env: Mapping to read from. Defaults to ``os.environ``.

        Returns:
            A validated Config.

        Raises:
            ConfigError: If any variable is missing or out of range. The
                message lists every problem found, not only the first.
        """
        source = dict(os.environ) if env is None else dict(env)
        problems: list[str] = []

        feed_url = source.get("FEED_URL", DEFAULT_FEED_URL).strip()
        if not feed_url.startswith(("http://", "https://")):
            problems.append(f"FEED_URL: {feed_url!r} is not an http(s) URL")

        poll_interval = _get_int(source, "POLL_INTERVAL_SECONDS", 30, problems, minimum=5)
        http_timeout = _get_int(source, "HTTP_TIMEOUT_SECONDS", 20, problems, minimum=1)

        bootstrap = _get_required(source, "KAFKA_BOOTSTRAP_SERVERS", problems)
        registry = _get_required(source, "SCHEMA_REGISTRY_URL", problems)

        record_dir_raw = source.get("RECORD_DIR", "").strip()
        record_dir = Path(record_dir_raw) if record_dir_raw else None

        config = cls(
            feed_url=feed_url,
            poll_interval_seconds=poll_interval,
            http_timeout_seconds=http_timeout,
            user_agent=source.get(
                "HTTP_USER_AGENT",
                "bvg-delay-stream/1.0 (+https://github.com/; portfolio project)",
            ),
            bootstrap_servers=bootstrap,
            schema_registry_url=registry,
            topic_stop_events=source.get("TOPIC_STOP_EVENTS", "stop-events"),
            topic_dlq=source.get("TOPIC_DLQ", "stop-events.dlq"),
            # Empty prefix means "publish the whole VBB area".
            stop_id_prefix=source.get("STOP_ID_PREFIX", "de:11000:"),
            max_event_age_seconds=_get_int(
                source, "MAX_EVENT_AGE_SECONDS", 3600, problems, minimum=60
            ),
            record_dir=record_dir,
            record_retention=_get_int(source, "RECORD_RETENTION", 2880, problems, minimum=1),
            metrics_port=_get_int(source, "METRICS_PORT", 9100, problems, 1024, 65535),
            log_level=source.get("LOG_LEVEL", "INFO").upper(),
        )

        if config.log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            problems.append(f"LOG_LEVEL: {config.log_level!r} is not a logging level")

        if problems:
            raise ConfigError("invalid configuration: " + "; ".join(problems))
        return config
