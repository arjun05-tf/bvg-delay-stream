"""HTTP client for the VBB GTFS-Realtime feed.

The feed is a full snapshot on every request, served without authentication.
The client issues a conditional GET so an unchanged snapshot costs a 304 and
no body, which matters because one snapshot is several megabytes.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import requests

from .errors import FatalError, RetryableError

_log = logging.getLogger(__name__)

# 408 and 429 are explicitly retryable; so is anything the origin or a proxy
# in front of it returns as a 5xx.
_RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504, 507, 509})


@dataclass(frozen=True, slots=True)
class FeedSnapshot:
    """One successfully fetched feed body."""

    payload: bytes
    etag: str | None
    fetched_at: float
    fetch_duration_seconds: float


class FeedClient:
    """Polls one GTFS-Realtime endpoint with conditional requests."""

    def __init__(self, url: str, timeout_seconds: int, user_agent: str) -> None:
        """Create a client.

        Args:
            url: Absolute http(s) URL of the GTFS-Realtime feed.
            timeout_seconds: Per request connect and read timeout.
            user_agent: Value sent in the User-Agent header. Public feeds are
                operated by people who need to know who is polling them.
        """
        self._url = url
        self._timeout = timeout_seconds
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": user_agent, "Accept": "*/*"})
        self._etag: str | None = None

    def fetch(self) -> FeedSnapshot | None:
        """Fetch the feed if it changed since the last call.

        Returns:
            A FeedSnapshot, or None when the server answered 304 Not Modified.

        Raises:
            RetryableError: Transient network or server failure.
            FatalError: The endpoint rejected the request in a way that
                retrying cannot fix, for example 403 or 404.
        """
        headers = {"If-None-Match": self._etag} if self._etag else {}
        started = time.monotonic()
        try:
            response = self._session.get(self._url, timeout=self._timeout, headers=headers)
        except requests.Timeout as exc:
            raise RetryableError(f"feed request timed out after {self._timeout}s") from exc
        except requests.ConnectionError as exc:
            raise RetryableError(f"feed connection failed: {exc}") from exc
        except requests.RequestException as exc:
            raise RetryableError(f"feed request failed: {exc}") from exc

        duration = time.monotonic() - started

        if response.status_code == 304:
            _log.debug("feed unchanged", extra={"status": 304, "duration_s": round(duration, 3)})
            return None

        if response.status_code in _RETRYABLE_STATUS:
            raise RetryableError(f"feed returned HTTP {response.status_code}")

        if response.status_code >= 400:
            raise FatalError(
                f"feed returned HTTP {response.status_code}, which retrying will not fix"
            )

        if not response.content:
            # A 200 with an empty body is not a parse problem, it is an
            # upstream hiccup. Treat it as transient.
            raise RetryableError("feed returned HTTP 200 with an empty body")

        self._etag = response.headers.get("ETag")
        return FeedSnapshot(
            payload=response.content,
            etag=self._etag,
            fetched_at=time.time(),
            fetch_duration_seconds=duration,
        )

    def close(self) -> None:
        """Release the underlying connection pool."""
        self._session.close()
