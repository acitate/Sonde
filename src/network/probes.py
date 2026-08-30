import time
from datetime import datetime

import httpx
from icmplib.ping import ping

from .models import ProbeResult


def endpoint_probe_http(
    endpoint: str, proxy: str | None, timeout: float
) -> ProbeResult:
    """Check whether an HTTP endpoint is reachable and measure latency.

    Args:
        endpoint (str): Endpoint to check.
        proxy (str | None, optional): Optional proxy:
            - None: direct connection
            - "http://127.0.0.1:8080": HTTP proxy
            - "socks5://127.0.0.1:1080": SOCKS5 proxy
        timeout (float, optional): Request timeout in seconds. Defaults to 5.

    Returns:
        ProbeResult
    """
    try:
        with httpx.Client(
            proxy=proxy, timeout=timeout, follow_redirects=True
        ) as client:
            start = time.perf_counter()

            response = client.head(endpoint)

            end = time.perf_counter()

            return ProbeResult(
                endpoint=endpoint,
                route="proxy" if proxy else "direct",
                timestamp=datetime.now(),
                reachable=True,
                latency_ms=end - start,
                method="http",
            )
    except httpx.HTTPError, OSError:
        return ProbeResult(
            endpoint=endpoint,
            route="proxy" if proxy else "direct",
            reachable=False,
            timestamp=datetime.now(),
            latency_ms=None,
            method="http",
        )


def endpoint_probe_icmp(endpoint: str, timeout: float) -> ProbeResult:
    """Check whether an endpoint is reachable using ICMP.

    Args:
        endpoint (str): Endpoint to check.
        timeout (float, optional): Timeout in seconds. Defaults to 2.

    Returns:
        ProbeResult: _description_
    """
    result = ping(address=endpoint, count=4, timeout=timeout, privileged=False)

    if result.is_alive:
        return ProbeResult(
            endpoint=endpoint,
            route="direct",
            timestamp=datetime.now(),
            reachable=True,
            latency_ms=result.avg_rtt,
            method="icmp",
        )
    else:
        return ProbeResult(
            endpoint=endpoint,
            route="direct",
            timestamp=datetime.now(),
            reachable=False,
            latency_ms=None,
            method="icmp",
        )