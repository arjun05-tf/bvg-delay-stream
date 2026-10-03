"""Pin schema registry compatibility for the StopEvent subject.

BACKWARD compatibility is what turns the evolution rules written in the proto
file into something the registry enforces. Without it, the rules are a comment.

Run once after the first message has been produced, which is when the subject
exists. Safe to run repeatedly.
"""

from __future__ import annotations

import logging
import os
import sys

import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
)
_log = logging.getLogger("set_compatibility")


def main() -> int:
    """Set subject level compatibility.

    Returns:
        Process exit code.
    """
    registry = os.environ.get("SCHEMA_REGISTRY_URL", "")
    if not registry:
        _log.error("SCHEMA_REGISTRY_URL is required")
        return 2
    subject = os.environ.get("SCHEMA_SUBJECT", "stop-events-value")
    level = os.environ.get("COMPATIBILITY_LEVEL", "BACKWARD")

    url = f"{registry.rstrip('/')}/config/{subject}"
    try:
        response = requests.put(
            url,
            json={"compatibility": level},
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            timeout=15,
        )
    except requests.RequestException as exc:
        _log.error("schema registry unreachable: %s", exc)
        return 1

    if response.status_code >= 400:
        _log.error(
            "registry rejected the compatibility change: HTTP %s %s",
            response.status_code,
            response.text[:200],
        )
        return 1

    _log.info("subject %s compatibility set to %s", subject, level)
    return 0


if __name__ == "__main__":
    sys.exit(main())
