"""Configuration validation tests."""

from __future__ import annotations

import pytest

from bvg_producer.config import DEFAULT_FEED_URL, Config
from bvg_producer.errors import ConfigError

MINIMAL = {
    "KAFKA_BOOTSTRAP_SERVERS": "redpanda:9092",
    "SCHEMA_REGISTRY_URL": "http://redpanda:8081",
}


def test_defaults_applied() -> None:
    config = Config.from_env(MINIMAL)
    assert config.feed_url == DEFAULT_FEED_URL
    assert config.poll_interval_seconds == 30
    assert config.stop_id_prefix == "de:11000:"
    assert config.record_dir is None


def test_missing_required_variables_are_all_reported() -> None:
    with pytest.raises(ConfigError) as excinfo:
        Config.from_env({})
    message = str(excinfo.value)
    assert "KAFKA_BOOTSTRAP_SERVERS" in message
    assert "SCHEMA_REGISTRY_URL" in message


def test_non_integer_poll_interval_is_rejected() -> None:
    with pytest.raises(ConfigError, match="POLL_INTERVAL_SECONDS"):
        Config.from_env({**MINIMAL, "POLL_INTERVAL_SECONDS": "thirty"})


def test_poll_interval_below_minimum_is_rejected() -> None:
    with pytest.raises(ConfigError, match="below the minimum"):
        Config.from_env({**MINIMAL, "POLL_INTERVAL_SECONDS": "1"})


def test_bad_feed_url_is_rejected() -> None:
    with pytest.raises(ConfigError, match="FEED_URL"):
        Config.from_env({**MINIMAL, "FEED_URL": "ftp://example.org/feed"})


def test_empty_prefix_means_whole_network() -> None:
    config = Config.from_env({**MINIMAL, "STOP_ID_PREFIX": ""})
    assert config.stop_id_prefix == ""
