"""Feed client tests: the retryable versus fatal split, and conditional GET."""

from __future__ import annotations

import pytest
import requests

from bvg_producer.errors import FatalError, RetryableError
from bvg_producer.feed_client import FeedClient


class _Response:
    def __init__(self, status_code: int, content: bytes = b"", etag: str | None = None) -> None:
        self.status_code = status_code
        self.content = content
        self.headers = {"ETag": etag} if etag else {}


class _Session:
    """Stand-in for requests.Session that replays a scripted list of outcomes."""

    def __init__(self, outcomes: list[object]) -> None:
        self.outcomes = outcomes
        self.calls: list[dict[str, str]] = []
        self.headers: dict[str, str] = {}

    def get(self, url: str, timeout: int, headers: dict[str, str]) -> _Response:
        self.calls.append(dict(headers))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        assert isinstance(outcome, _Response)
        return outcome

    def close(self) -> None:
        return None

    def update(self, _values: dict[str, str]) -> None:
        return None


def _client(outcomes: list[object]) -> tuple[FeedClient, _Session]:
    client = FeedClient("https://example.org/data", 5, "test-agent")
    session = _Session(outcomes)
    client._session = session  # noqa: SLF001 - injection point for the test
    return client, session


def test_successful_fetch_returns_payload_and_stores_etag() -> None:
    client, session = _client([_Response(200, b"body", etag='W/"abc"')])
    snapshot = client.fetch()
    assert snapshot is not None
    assert snapshot.payload == b"body"
    assert snapshot.etag == 'W/"abc"'
    assert session.calls[0] == {}


def test_second_fetch_sends_if_none_match() -> None:
    client, session = _client(
        [_Response(200, b"body", etag='W/"abc"'), _Response(304)]
    )
    client.fetch()
    assert client.fetch() is None
    assert session.calls[1] == {"If-None-Match": 'W/"abc"'}


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_transient_statuses_are_retryable(status: int) -> None:
    client, _ = _client([_Response(status)])
    with pytest.raises(RetryableError):
        client.fetch()


@pytest.mark.parametrize("status", [400, 401, 403, 404, 410])
def test_client_errors_are_fatal(status: int) -> None:
    client, _ = _client([_Response(status)])
    with pytest.raises(FatalError):
        client.fetch()


def test_timeout_is_retryable() -> None:
    client, _ = _client([requests.Timeout("too slow")])
    with pytest.raises(RetryableError, match="timed out"):
        client.fetch()


def test_connection_error_is_retryable() -> None:
    client, _ = _client([requests.ConnectionError("reset")])
    with pytest.raises(RetryableError, match="connection failed"):
        client.fetch()


def test_empty_body_is_retryable_not_malformed() -> None:
    client, _ = _client([_Response(200, b"")])
    with pytest.raises(RetryableError, match="empty body"):
        client.fetch()
