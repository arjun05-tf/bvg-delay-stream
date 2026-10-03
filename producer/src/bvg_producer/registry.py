"""Schema registry wiring.

Redpanda ships a Confluent-compatible schema registry, so the standard
Confluent protobuf serialiser works unchanged. The serialiser registers the
schema under ``<topic>-value`` on first use and prefixes every message with
the Confluent wire format header: one magic byte, a four byte schema id, then
the message index path.

Compatibility is set to BACKWARD at the subject level by
``tools/set_compatibility.py``, which is what makes the evolution rules in
proto/delay/v1/stop_event.proto enforceable instead of aspirational.
"""

from __future__ import annotations

import logging

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer

from .errors import FatalError
from .generated.delay.v1 import stop_event_pb2

_log = logging.getLogger(__name__)


def build_serializer(schema_registry_url: str) -> ProtobufSerializer:
    """Create a protobuf serialiser bound to the StopEvent message type.

    Args:
        schema_registry_url: Base URL of the schema registry.

    Returns:
        A serialiser that registers and caches the StopEvent schema.

    Raises:
        FatalError: The registry is unreachable or rejects the client config.
            Nothing downstream can work without it, so this does not retry.
    """
    try:
        client = SchemaRegistryClient({"url": schema_registry_url})
        # Touch the registry now, so a bad URL fails at startup and not on the
        # first message half an hour later.
        client.get_subjects()
    except Exception as exc:  # noqa: BLE001 - client raises several unrelated types
        raise FatalError(f"schema registry at {schema_registry_url} is unusable: {exc}") from exc

    _log.info("schema registry reachable", extra={"url": schema_registry_url})
    return ProtobufSerializer(
        stop_event_pb2.StopEvent,
        client,
        {"use.deprecated.format": False, "auto.register.schemas": True},
    )
