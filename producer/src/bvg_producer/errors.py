"""Error taxonomy for the producer.

The whole retry policy of this service rests on one distinction: an error is
either worth trying again in a few seconds, or it is not. Mixing the two is
how pollers end up hammering a broken endpoint for hours, or dying on a
transient 503. Every error raised inside this package is one of the two
classes below, so the main loop never has to inspect error strings.
"""

from __future__ import annotations


class ProducerError(Exception):
    """Base class for every error this package raises."""


class RetryableError(ProducerError):
    """A transient failure. The caller should back off and try again.

    Network timeouts, connection resets, HTTP 429 and HTTP 5xx.
    """


class FatalError(ProducerError):
    """A failure that will not fix itself. The caller should stop.

    Bad configuration, HTTP 401/403/404, an unusable schema registry.
    """


class ConfigError(FatalError):
    """Configuration is missing or invalid."""


class MalformedFeedError(ProducerError):
    """The feed payload could not be parsed as a GTFS-Realtime FeedMessage.

    Neither retryable nor fatal: the payload goes to the dead letter topic and
    the loop continues with the next poll.
    """

    def __init__(self, message: str, payload: bytes) -> None:
        super().__init__(message)
        self.payload = payload
