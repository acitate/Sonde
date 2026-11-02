"""Tests for network.speed (async download speed measurement).

All HTTP I/O is stubbed with small fakes: FakeAsyncClient serves canned
streaming responses per URL and records stream() calls. Timing is made
deterministic by replacing network.speed's `time` module with a fake
clock, so Mbps values can be asserted exactly.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx
import pytest

from network.speed import _download_worker, download_speed_test


class FakeStreamResponse:
    """Streaming response yielding configured chunks."""

    def __init__(self, chunks, error=None):
        self._chunks = chunks
        self._error = error

    def raise_for_status(self):
        if self._error is not None:
            raise self._error

    async def aiter_bytes(self):
        for chunk in self._chunks:
            yield chunk


class FakeStreamContext:
    """Async context manager for a streaming response."""

    def __init__(self, response):
        self._response = response

    async def __aenter__(self):
        return self._response

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeAsyncClient:
    """Stand-in for httpx.AsyncClient serving canned stream responses.

    `plans` maps each URL to a list of per-connection plans consumed in
    order, one per stream() call. A plan is either a list of byte chunks
    or an exception instance to raise from raise_for_status(). Running
    out of plans raises so miscounted tests fail loudly.
    """

    def __init__(self, plans):
        self._plans = {url: list(queue) for url, queue in plans.items()}
        self.stream_calls = []

    def stream(self, method, url=None):
        self.stream_calls.append(url)
        if url not in self._plans:
            raise AssertionError(f"unexpected stream URL {url!r}")
        if not self._plans[url]:
            raise AssertionError(f"no plan left for URL {url!r}")
        plan = self._plans[url].pop(0)
        if isinstance(plan, Exception):
            return FakeStreamContext(FakeStreamResponse([], error=plan))
        return FakeStreamContext(FakeStreamResponse(plan))

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


def status_error() -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "http://files.example/big.bin")
    response = httpx.Response(404, request=request)
    return httpx.HTTPStatusError(
        "Client error '404 Not Found'", request=request, response=response
    )


def fake_clock(values):
    """Replacement for the time module inside network.speed."""
    iterator = iter(values)
    return SimpleNamespace(perf_counter=lambda: next(iterator))


async def run_with_fake(fake_client, clock_values, **kwargs):
    """Patch httpx.AsyncClient/time and await download_speed_test."""
    client_cls = MagicMock(return_value=fake_client)
    defaults = dict(
        urls=["http://files.example/big.bin"],
        runs=1,
        connections=1,
        bytes_per_connection=1_000_000,
        proxy=None,
        timeout=30.0,
    )
    defaults.update(kwargs)
    with (
        patch("network.speed.httpx.AsyncClient", client_cls),
        patch("network.speed.time", fake_clock(clock_values)),
    ):
        return await download_speed_test(**defaults), client_cls


URL = "http://files.example/big.bin"


class TestDownloadWorker:
    async def test_returns_bytes_received_when_under_max(self):
        client = FakeAsyncClient(
            {URL: [[b"a" * 400, b"b" * 300, b"c" * 250]]}
        )

        received = await _download_worker(client, url=URL, max_bytes=1000)

        assert received == 950

    async def test_truncates_when_chunks_exceed_max(self):
        client = FakeAsyncClient(
            {URL: [[b"a" * 400, b"b" * 300, b"c" * 500]]}
        )

        received = await _download_worker(client, url=URL, max_bytes=1000)

        assert received == 1000

    async def test_stops_exactly_at_boundary(self):
        # A trailing chunk beyond the boundary exercises the loop's
        # early break once max_bytes is reached.
        client = FakeAsyncClient({URL: [[b"a" * 400, b"b" * 600, b"c" * 50]]})

        received = await _download_worker(client, url=URL, max_bytes=1000)

        assert received == 1000

    async def test_empty_stream_returns_zero(self):
        client = FakeAsyncClient({URL: [[]]})

        received = await _download_worker(client, url=URL, max_bytes=1000)

        assert received == 0

    async def test_http_status_error_propagates(self):
        client = FakeAsyncClient({URL: [status_error()]})

        with pytest.raises(httpx.HTTPStatusError):
            await _download_worker(client, url=URL, max_bytes=1000)

    async def test_stream_called_with_get_and_url(self):
        client = FakeAsyncClient({URL: [[b"a" * 10]]})

        await _download_worker(client, url=URL, max_bytes=10)

        assert client.stream_calls == [URL]


class TestDownloadSpeedTest:
    async def test_empty_urls_raises_before_client_creation(self):
        client_cls = MagicMock()

        with patch("network.speed.httpx.AsyncClient", client_cls):
            with pytest.raises(ValueError, match="zero URLs"):
                await download_speed_test(
                    urls=[],
                    runs=2,
                    connections=2,
                    bytes_per_connection=1024,
                    proxy=None,
                    timeout=30.0,
                )

        client_cls.assert_not_called()

    async def test_happy_path_exact_math(self):
        # 2 connections x 1,000,000 bytes in 1.0 s -> 16.0 Mbps.
        fake = FakeAsyncClient(
            {URL: [[b"x" * 1_000_000], [b"x" * 1_000_000]]}
        )

        result, _ = await run_with_fake(
            fake, [0.0, 1.0], connections=2
        )

        assert result.median_speed_mbps == pytest.approx(16.0)
        assert result.run_speeds_mbps == pytest.approx([16.0])
        assert result.successful_connections == 2
        assert result.total_connections == 2
        assert result.route == "direct"

    async def test_median_across_runs_odd_count(self):
        # Elapsed 1s, 2s, 0.5s -> run speeds 8.0, 4.0, 16.0 -> median 8.0.
        fake = FakeAsyncClient(
            {URL: [[b"x" * 1_000_000]] * 3}
        )

        result, _ = await run_with_fake(
            fake, [0.0, 1.0, 10.0, 12.0, 100.0, 100.5], runs=3
        )

        assert result.run_speeds_mbps == pytest.approx([8.0, 4.0, 16.0])
        assert result.median_speed_mbps == pytest.approx(8.0)
        assert result.total_connections == 3

    async def test_median_across_runs_even_count(self):
        # Elapsed 1s, 2s -> run speeds 8.0, 4.0 -> median 6.0.
        fake = FakeAsyncClient(
            {URL: [[b"x" * 1_000_000]] * 2}
        )

        result, _ = await run_with_fake(fake, [0.0, 1.0, 10.0, 12.0], runs=2)

        assert result.run_speeds_mbps == pytest.approx([8.0, 4.0])
        assert result.median_speed_mbps == pytest.approx(6.0)

    async def test_urls_assigned_round_robin(self):
        other = "http://mirror.example/big.bin"
        fake = FakeAsyncClient(
            {
                URL: [[b"x" * 10]] * 3,
                other: [[b"x" * 10]] * 2,
            }
        )

        await run_with_fake(
            fake,
            [0.0, 1.0],
            urls=[URL, other],
            connections=5,
            bytes_per_connection=10,
        )

        assert fake.stream_calls == [URL, other, URL, other, URL]

    async def test_partial_failure_counts_only_successful_bytes(self):
        # Connection 1 delivers 500,000 bytes in 0.5 s -> 8.0 Mbps;
        # connection 2 fails outright.
        fake = FakeAsyncClient(
            {URL: [[b"x" * 500_000], httpx.ConnectError("refused")]}
        )

        result, _ = await run_with_fake(fake, [0.0, 0.5], connections=2)

        assert result.run_speeds_mbps == pytest.approx([8.0])
        assert result.median_speed_mbps == pytest.approx(8.0)
        assert result.successful_connections == 1
        assert result.total_connections == 2

    async def test_all_connections_fail(self):
        fake = FakeAsyncClient(
            {URL: [httpx.ConnectError("refused"), httpx.ConnectError("refused")]}
        )

        result, _ = await run_with_fake(fake, [0.0, 0.5], connections=2)

        assert result.run_speeds_mbps == []
        assert result.median_speed_mbps == 0.0
        assert result.successful_connections == 0
        assert result.total_connections == 2

    async def test_zero_byte_success_counted_in_total_only(self):
        # Both connections "succeed" but transfer nothing: totals move,
        # successful count and speeds stay empty (commit 583b414).
        fake = FakeAsyncClient({URL: [[], []]})

        result, _ = await run_with_fake(fake, [0.0, 0.5], connections=2)

        assert result.run_speeds_mbps == []
        assert result.median_speed_mbps == 0.0
        assert result.successful_connections == 0
        assert result.total_connections == 2

    async def test_zero_byte_connection_does_not_dilute_speed(self):
        # One real connection (1,000,000 bytes in 1 s = 8 Mbps) plus one
        # zero-byte connection: speed comes from real bytes only.
        fake = FakeAsyncClient({URL: [[b"x" * 1_000_000], []]})

        result, _ = await run_with_fake(
            fake, [0.0, 1.0], connections=2
        )

        assert result.run_speeds_mbps == pytest.approx([8.0])
        assert result.successful_connections == 1
        assert result.total_connections == 2

    async def test_route_and_client_kwargs_via_proxy(self):
        fake = FakeAsyncClient({URL: [[b"x" * 10]]})

        result, client_cls = await run_with_fake(
            fake,
            [0.0, 1.0],
            proxy="socks5://127.0.0.1:1080",
            timeout=12.5,
            bytes_per_connection=10,
        )

        assert result.route == "proxy"
        client_cls.assert_called_once_with(
            proxy="socks5://127.0.0.1:1080",
            timeout=12.5,
            follow_redirects=True,
        )

    async def test_client_kwargs_direct(self):
        fake = FakeAsyncClient({URL: [[b"x" * 10]]})

        _, client_cls = await run_with_fake(
            fake, [0.0, 1.0], timeout=7.0, bytes_per_connection=10
        )

        client_cls.assert_called_once_with(
            proxy=None, timeout=7.0, follow_redirects=True
        )

    async def test_timestamp_is_timezone_aware_utc(self):
        fake = FakeAsyncClient({URL: [[b"x" * 10]]})

        result, _ = await run_with_fake(fake, [0.0, 1.0], bytes_per_connection=10)

        assert result.timestamp.tzinfo is not None
        assert result.timestamp.utcoffset().total_seconds() == 0

    async def test_zero_runs_returns_empty_result(self):
        fake = FakeAsyncClient({URL: []})

        result, _ = await run_with_fake(fake, [], runs=0)

        assert result.run_speeds_mbps == []
        assert result.median_speed_mbps == 0.0
        assert result.successful_connections == 0
        assert result.total_connections == 0

    async def test_zero_connections_returns_empty_result(self):
        fake = FakeAsyncClient({URL: []})

        result, _ = await run_with_fake(fake, [0.0, 0.5], connections=0)

        assert result.run_speeds_mbps == []
        assert result.median_speed_mbps == 0.0
        assert result.total_connections == 0
