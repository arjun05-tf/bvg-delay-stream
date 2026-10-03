"""Kafka publishing for StopEvent messages and dead letter records."""

from __future__ import annotations

import logging
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Message, Producer
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from . import metrics
from .errors import RetryableError
from .generated.delay.v1 import stop_event_pb2
from .transform import event_key

_log = logging.getLogger(__name__)


def _delivery_report(err: KafkaError | None, msg: Message) -> None:
    """Count the outcome of one delivery.

    librdkafka calls this from its background thread after the broker
    acknowledges the message or gives up on it. Retries inside librdkafka
    have already happened by the time an error reaches here, so an error is
    final for that message.
    """
    if err is None:
        metrics.EVENTS_DELIVERED.inc()
        return
    metrics.DELIVERY_FAILURES.inc()
    _log.error(
        "kafka delivery failed",
        extra={"topic": msg.topic(), "error": str(err), "code": err.code()},
    )


class EventPublisher:
    """Publishes StopEvents and dead letter records to Kafka."""

    def __init__(
        self,
        bootstrap_servers: str,
        serializer: ProtobufSerializer,
        topic: str,
        dlq_topic: str,
        extra_config: dict[str, Any] | None = None,
    ) -> None:
        """Create a publisher.

        Args:
            bootstrap_servers: Comma separated broker list.
            serializer: Protobuf serialiser for StopEvent.
            topic: Destination topic for StopEvents.
            dlq_topic: Destination topic for records that could not be parsed
                or transformed.
            extra_config: Additional librdkafka settings, merged last.
        """
        config: dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers,
            # Idempotent production: no duplicates and no reordering on
            # internal retries, which the replay proof depends on.
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "zstd",
            "linger.ms": 50,
            "batch.size": 262_144,
            "message.timeout.ms": 120_000,
            "retries": 10,
            "queue.buffering.max.messages": 500_000,
            "queue.buffering.max.kbytes": 262_144,
        }
        if extra_config:
            config.update(extra_config)
        self._producer = Producer(config)
        self._serializer = serializer
        self._topic = topic
        self._dlq_topic = dlq_topic
        self._context = SerializationContext(topic, MessageField.VALUE)

    def publish(self, event: stop_event_pb2.StopEvent) -> None:
        """Queue one StopEvent for delivery.

        Args:
            event: The event to publish.

        Raises:
            RetryableError: The local queue is full. The caller should poll
                for delivery callbacks and try again rather than drop data.
        """
        payload = self._serializer(event, self._context)
        try:
            self._producer.produce(
                topic=self._topic,
                key=event_key(event),
                value=payload,
                on_delivery=_delivery_report,
            )
        except BufferError as exc:
            raise RetryableError("kafka local queue is full") from exc
        except KafkaException as exc:
            raise RetryableError(f"kafka produce failed: {exc}") from exc
        metrics.EVENTS_PUBLISHED.inc()

    def publish_dead_letter(self, reason: str, payload: bytes, detail: str) -> None:
        """Write one unusable record to the dead letter topic.

        The original bytes are kept verbatim. Reason and detail travel as
        headers so an operator can filter the topic without decoding bodies.

        Args:
            reason: Short machine readable cause, for example
                ``"malformed_feed"``.
            payload: The bytes that could not be processed.
            detail: Human readable context, for example the trip id.
        """
        try:
            self._producer.produce(
                topic=self._dlq_topic,
                value=payload,
                headers=[("reason", reason.encode()), ("detail", detail.encode()[:1024])],
                on_delivery=_delivery_report,
            )
        except (BufferError, KafkaException) as exc:
            # Losing a dead letter must not take the service down, but it has
            # to be loud.
            _log.error("dead letter publish failed", extra={"reason": reason, "error": str(exc)})
            return
        metrics.DLQ_MESSAGES.labels(reason=reason).inc()

    def poll(self, timeout_seconds: float = 0.0) -> int:
        """Serve delivery callbacks.

        Args:
            timeout_seconds: How long to block waiting for callbacks.

        Returns:
            Number of callbacks served.
        """
        return int(self._producer.poll(timeout_seconds))

    def flush(self, timeout_seconds: float = 30.0) -> int:
        """Block until the queue drains or the timeout expires.

        Args:
            timeout_seconds: Upper bound on the wait.

        Returns:
            Number of messages still queued. Non-zero means data was not
            acknowledged and the caller should treat shutdown as unclean.
        """
        return int(self._producer.flush(timeout_seconds))
