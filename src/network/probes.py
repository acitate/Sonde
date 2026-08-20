import time
from datetime import datetime

import httpx

from .models import ProbeResult


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
