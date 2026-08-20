import time
from datetime import date, datetime
from traceback import print_tb

import httpx
from icmplib.ping import ping
from models import ProbeResult  # ty: ignore[unresolved-import]


def endpoint_probe_http(
    endpoint: str, proxy: str | None = None, timeout: float = 5
) -> ProbeResult:
    try:
        with httpx.Client(
            proxy=proxy, timeout=timeout, follow_redirects=True
        ) as client:
            start = time.perf_counter()

            response = client.get(endpoint)

            end = time.perf_counter()

            return ProbeResult(
                endpoint=endpoint,
                route="proxy" if proxy else "direct",
                timestamp=datetime.now(),
                reachable=True,
                latency=end - start,
                method="http",
            )
    except httpx.HTTPError, OSError:
        return ProbeResult(
            endpoint=endpoint,
            route="proxy" if proxy else "direct",
            reachable=False,
            timestamp=datetime.now(),
            latency=None,
            method="http",
        )


def endpoint_probe_icmp(endpoint: str, timeout: float = 2) -> ProbeResult:
    result = ping(address=endpoint, count=4, timeout=timeout, privileged=False)

    if result.is_alive:
        return ProbeResult(
            endpoint=endpoint,
            route="direct",
            timestamp=datetime.now(),
            reachable=True,
            latency=result.avg_rtt,
            method="icmp",
        )
    else:
        return ProbeResult(
            endpoint=endpoint,
            route="direct",
            timestamp=datetime.now(),
            reachable=False,
            latency=None,
            method="icmp",
        )