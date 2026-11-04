import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from monitor.monitor import Monitor
from network.models import ProbeResult


class FakeRepository:
    def __init__(self, checkpoints, proxies=None):
        self.checkpoints = checkpoints
        self.proxies = proxies or {}
        self.saved_results = []

    def get_enabled_latency_checkpoints(self):
        return self.checkpoints

    def get_proxy(self, proxy_id):
        return self.proxies.get(proxy_id)

    def save_latency_probe(self, checkpoint_id, result, proxy_id=None):
        self.saved_results.append(
            (checkpoint_id, result, proxy_id)
        )


class TestMonitor(unittest.IsolatedAsyncioTestCase):

    async def test_http_latency_check(self):
        checkpoint = SimpleNamespace(
            id=1,
            endpoint="https://example.com",
            method="http",
            timeout=5.0,
            proxy_id=None,
            enabled=True,
        )

        repository = FakeRepository([checkpoint])
        monitor = Monitor(repository)

        fake_result = ProbeResult(
            endpoint="https://example.com",
            route="direct",
            method="http",
            timestamp=datetime.now(),
            reachable=True,
            latency_ms=120.5,
        )

        with patch(
            "monitor.monitor.endpoint_probe_http",
            return_value=fake_result,
        ) as mock_probe:

            await monitor.run_latency_checks()

        mock_probe.assert_called_once_with(
            "https://example.com",
            None,
            5.0,
        )

        self.assertEqual(len(repository.saved_results), 1)

        checkpoint_id, result, proxy_id = repository.saved_results[0]

        self.assertEqual(checkpoint_id, 1)
        self.assertIsNone(proxy_id)
        self.assertTrue(result.reachable)

        self.assertEqual(result.timestamp.tzinfo, timezone.utc)

    async def test_proxy_resolution(self):
        checkpoint = SimpleNamespace(
            id=2,
            endpoint="https://example.com",
            method="http",
            timeout=5.0,
            proxy_id=10,
            enabled=True,
        )

        proxy = SimpleNamespace(
            id=10,
            url="http://127.0.0.1:8080",
        )

        repository = FakeRepository(
            checkpoints=[checkpoint],
            proxies={10: proxy},
        )

        monitor = Monitor(repository)

        fake_result = ProbeResult(
            endpoint="https://example.com",
            route="proxy",
            method="http",
            timestamp=datetime.now(),
            reachable=True,
            latency_ms=100.0,
        )

        with patch(
            "monitor.monitor.endpoint_probe_http",
            return_value=fake_result,
        ) as mock_probe:

            await monitor.run_latency_checks()

        mock_probe.assert_called_once_with(
            "https://example.com",
            "http://127.0.0.1:8080",
            5.0,
        )

        self.assertEqual(len(repository.saved_results), 1)
        self.assertEqual(repository.saved_results[0][2], 10)

    async def test_failure_does_not_stop_other_checks(self):
        checkpoints = [
            SimpleNamespace(
                id=1,
                endpoint="https://first.example.com",
                method="http",
                timeout=5.0,
                proxy_id=None,
                enabled=True,
            ),
            SimpleNamespace(
                id=2,
                endpoint="https://second.example.com",
                method="http",
                timeout=5.0,
                proxy_id=None,
                enabled=True,
            ),
        ]

        repository = FakeRepository(checkpoints)
        monitor = Monitor(repository)

        fake_result = ProbeResult(
            endpoint="https://second.example.com",
            route="direct",
            method="http",
            timestamp=datetime.now(),
            reachable=True,
            latency_ms=90.0,
        )

        with patch(
            "monitor.monitor.endpoint_probe_http",
            side_effect=[
                RuntimeError("Test error"),
                fake_result,
            ],
        ):

            await monitor.run_latency_checks()

        self.assertEqual(len(repository.saved_results), 1)
        self.assertEqual(repository.saved_results[0][0], 2)


if __name__ == "__main__":
    unittest.main()
