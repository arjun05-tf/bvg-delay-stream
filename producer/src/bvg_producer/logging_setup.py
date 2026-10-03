"""Structured JSON logging.

One line of JSON per event on stdout, which is what every log shipper in the
compose file and in CI expects. Extra context is passed through the standard
`extra=` mechanism so call sites stay plain stdlib logging.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import sys
from typing import Any

_RESERVED = frozenset(
    logging.LogRecord("", 0, "", 0, "", None, None).__dict__.keys()
) | {"asctime", "message", "taskName"}


class JsonFormatter(logging.Formatter):
    """Render a log record as a single JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": dt.datetime.fromtimestamp(record.created, tz=dt.UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED:
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, separators=(",", ":"))


def configure_logging(level: str) -> None:
    """Install the JSON formatter on the root logger.

    Args:
        level: Logging level name, for example "INFO". Case insensitive.
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())
    # The Kafka client logs every metadata refresh at INFO. Useful at DEBUG.
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)
