"""Decoder tests."""

from __future__ import annotations

import pytest
from google.transit import gtfs_realtime_pb2

from bvg_producer.decode import parse_feed
from bvg_producer.errors import MalformedFeedError


def test_round_trip(feed: gtfs_realtime_pb2.FeedMessage) -> None:
    parsed = parse_feed(feed.SerializeToString())
    assert parsed.header.timestamp == feed.header.timestamp
    assert len(parsed.entity) == len(feed.entity)


def test_garbage_payload_raises_with_payload_attached() -> None:
    payload = b"\xff\xfe<html>not protobuf</html>"
    with pytest.raises(MalformedFeedError) as excinfo:
        parse_feed(payload)
    assert excinfo.value.payload == payload


def test_missing_header_timestamp_is_malformed() -> None:
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    with pytest.raises(MalformedFeedError, match="no timestamp"):
        parse_feed(message.SerializeToString())
