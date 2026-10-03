"""GTFS-Realtime payload decoding."""

from __future__ import annotations

from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

from .errors import MalformedFeedError


def parse_feed(payload: bytes) -> gtfs_realtime_pb2.FeedMessage:
    """Parse raw bytes into a GTFS-Realtime FeedMessage.

    Args:
        payload: Response body of the feed endpoint.

    Returns:
        The parsed FeedMessage.

    Raises:
        MalformedFeedError: The bytes are not a valid FeedMessage, or the
            header is missing a timestamp, which would leave every event
            without a snapshot time to compare against.
    """
    message = gtfs_realtime_pb2.FeedMessage()
    try:
        message.ParseFromString(payload)
    except DecodeError as exc:
        raise MalformedFeedError(f"payload is not a FeedMessage: {exc}", payload) from exc

    if not message.header.timestamp:
        raise MalformedFeedError("feed header carries no timestamp", payload)
    return message
