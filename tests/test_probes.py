"""Tests for network.probes (HTTP and ICMP endpoint probing).

All network I/O is mocked: httpx.Client is replaced with MagicMock and
icmplib's ping is patched at its point of use in the module namespace.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import httpx
import pytest

from network.probes import endpoint_probe_http, endpoint_probe_icmp

UTC = ZoneInfo("UTC")


def make_client_mock() -> MagicMock:
    """Mock httpx.Client whose head() succeeds by default."""
    client = MagicMock()
    client.head.return_value = MagicMock(status_code=200)
    client.__enter__.return_value = client
    client.__exit__.return_value = False
    return client


def assert_unreachable(result, endpoint: str, route: str, method: str) -> None:
    assert result.reachable is False
    assert result.latency_ms is None
    assert result.endpoint == endpoint
    assert result.route == route
    assert result.method == method
    assert result.error is None
    assert result.timestamp.tzinfo is not None


class TestEndpointProbeHttp:
    def test_success_direct(self):
        client = make_client_mock()

        with patch("network.probes.httpx.Client", return_value=client):
            result = endpoint_probe_http(
                "https://example.com", proxy=None, timeout=5.0
            )

        assert result.reachable is True
        assert result.endpoint == "https://example.com"
        assert result.route == "direct"
        assert result.method == "http"
        assert result.error is None
        assert isinstance(result.latency_ms, float)
        assert result.latency_ms >= 0
        assert result.timestamp.tzinfo is not None
        assert result.timestamp.utcoffset().total_seconds() == 0

    @pytest.mark.parametrize(
        "proxy",
        ["http://127.0.0.1:8080", "socks5://127.0.0.1:1080"],
    )
    def test_success_via_proxy(self, proxy):
        client = make_client_mock()

        with patch("network.probes.httpx.Client", return_value=client):
            result = endpoint_probe_http(
                "https://example.com", proxy=proxy, timeout=5.0
            )

        assert result.reachable is True
        assert result.route == "proxy"

    def test_client_constructed_with_expected_kwargs(self):
        client = make_client_mock()

        with patch(
            "network.probes.httpx.Client", return_value=client
        ) as client_cls:
            endpoint_probe_http(
                "https://example.com",
                proxy="socks5://127.0.0.1:1080",
                timeout=3.5,
            )

        client_cls.assert_called_once_with(
            proxy="socks5://127.0.0.1:1080",
            timeout=3.5,
            follow_redirects=True,
        )

    def test_head_called_with_endpoint(self):
        client = make_client_mock()

        with patch("network.probes.httpx.Client", return_value=client):
            endpoint_probe_http("https://example.com", proxy=None, timeout=5.0)

        client.head.assert_called_once_with("https://example.com")

    def test_latency_is_measured_around_head_call(self):
        client = make_client_mock()

        with (
            patch("network.probes.httpx.Client", return_value=client),
            patch(
                "network.probes.time.perf_counter",
                side_effect=[1.0, 1.5],
            ),
        ):
            result = endpoint_probe_http(
                "https://example.com", proxy=None, timeout=5.0
            )

        assert result.latency_ms == pytest.approx(500.0)

    @pytest.mark.parametrize(
        "exc",
        [
            httpx.ConnectError("connection refused"),
            httpx.ConnectTimeout("timed out"),
            httpx.ReadTimeout("read timed out"),
            httpx.HTTPError("generic http error"),
        ],
    )
    def test_httpx_errors_report_unreachable(self, exc):
        client = make_client_mock()
        client.head.side_effect = exc

        with patch("network.probes.httpx.Client", return_value=client):
            result = endpoint_probe_http(
                "https://example.com", proxy=None, timeout=5.0
            )

        assert_unreachable(result, "https://example.com", "direct", "http")

    def test_os_error_reports_unreachable(self):
        client = make_client_mock()
        client.head.side_effect = OSError("network is unreachable")

        with patch("network.probes.httpx.Client", return_value=client):
            result = endpoint_probe_http(
                "https://example.com", proxy=None, timeout=5.0
            )

        assert_unreachable(result, "https://example.com", "direct", "http")

    def test_os_error_from_client_construction_reports_unreachable(self):
        with patch(
            "network.probes.httpx.Client",
            side_effect=OSError("cannot create client"),
        ):
            result = endpoint_probe_http(
                "https://example.com", proxy=None, timeout=5.0
            )

        assert_unreachable(result, "https://example.com", "direct", "http")

    def test_unexpected_exceptions_propagate(self):
        client = make_client_mock()
        client.head.side_effect = ValueError("not an http error")

        with patch("network.probes.httpx.Client", return_value=client):
            with pytest.raises(ValueError, match="not an http error"):
                endpoint_probe_http(
                    "https://example.com", proxy=None, timeout=5.0
                )

    def test_failure_via_proxy_keeps_proxy_route(self):
        client = make_client_mock()
        client.head.side_effect = httpx.ConnectError("proxy refused")

        with patch("network.probes.httpx.Client", return_value=client):
            result = endpoint_probe_http(
                "https://example.com",
                proxy="http://127.0.0.1:8080",
                timeout=5.0,
            )

        assert_unreachable(
            result, "https://example.com", "proxy", "http"
        )


class TestEndpointProbeIcmp:
    @pytest.mark.parametrize(
        "endpoint, avg_rtt",
        [
            ("192.0.2.1", 23.4),
            ("example.com", 0.0),
        ],
    )
    def test_alive(self, endpoint, avg_rtt):
        ping_result = SimpleNamespace(is_alive=True, avg_rtt=avg_rtt)

        with patch("network.probes.ping", return_value=ping_result):
            result = endpoint_probe_icmp(endpoint, timeout=2.0)

        assert result.reachable is True
        assert result.endpoint == endpoint
        assert result.route == "direct"
        assert result.method == "icmp"
        assert result.latency_ms == avg_rtt
        assert result.error is None
        assert result.timestamp.tzinfo is not None

    def test_dead(self):
        ping_result = SimpleNamespace(is_alive=False, avg_rtt=None)

        with patch("network.probes.ping", return_value=ping_result):
            result = endpoint_probe_icmp("192.0.2.1", timeout=2.0)

        assert_unreachable(result, "192.0.2.1", "direct", "icmp")

    def test_ping_called_with_expected_arguments(self):
        ping_result = SimpleNamespace(is_alive=True, avg_rtt=10.0)

        with patch("network.probes.ping", return_value=ping_result) as ping_mock:
            endpoint_probe_icmp("192.0.2.1", timeout=1.5)

        ping_mock.assert_called_once_with(
            address="192.0.2.1",
            count=4,
            timeout=1.5,
            privileged=False,
        )

    def test_ping_exceptions_propagate(self):
        # icmplib raises NameError for unresolvable hostnames; probes
        # currently lets it escape.
        with patch(
            "network.probes.ping", side_effect=NameError("unknown host")
        ):
            with pytest.raises(NameError, match="unknown host"):
                endpoint_probe_icmp("nonexistent.invalid", timeout=2.0)
