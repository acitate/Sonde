import asyncio
import statistics
import time
from datetime import datetime
from operator import is_none

import httpx

from .models import SpeedTestResult


async def _download_worker(client: httpx.AsyncClient, url: str, max_bytes) -> int:
    """Download worker. Download up to `max_bytes` from provided URL.

    Args:
        client (httpx.AsyncClient): Asynchronous HTTP client to use.
        url (str): URL to the target file.
        max_bytes (_type_): Number of bytes to download.

    Returns:
        int: Number of bytes received.
    """

    received = 0

    async with client.stream("GET", url=url) as response:
        response.raise_for_status()

        async for chunk in response.aiter_bytes():
            remaining = max_bytes - received
            if remaining <= 0:
                break

            received += len(chunk[:remaining])

    return received


async def download_speed_test(
    urls: list[str],
    runs: int,
    connections: int,
    bytes_per_connection: int,
    proxy: str | None,
    timeout: float,
) -> SpeedTestResult:
    """Measure practical download speed in Mbps.

    The returned speed is the median aggregate throughput of all
    successful runs.

    A run is considered successful if at least one connection
    successfully transfers data.

    Args:
        urls (list[str]): URLs of sufficiently large files. Treated as a pool. Assigned to connections in round-robin.
        runs (int, optional): Number of sequential runs. Defaults to 2.
        connections (int, optional): Number of concurrent connections to start in each run. Defaults to 2.
        bytes_per_connection (int, optional): Maximum bytes to download in each connection. Defaults to 5*1024*1024 = 5MB
        proxy (str | None, optional): Optional socks/http proxy. Defaults to None.
        timeout (float, optional): Download timeout in seconds. Defaults to 30.

    Returns:
        SpeedTestResult
    """

    speeds = []
    successful_connections = 0
    total_connections = 0

    async with httpx.AsyncClient(
        proxy=proxy,
        timeout=timeout,
        follow_redirects=True,
    ) as client:
        for run in range(runs):
            test_urls = [urls[i % len(urls)] for i in range(connections)]

            tasks = [
                _download_worker(client, url=url, max_bytes=bytes_per_connection)
                for url in test_urls
            ]

            start = time.perf_counter()

            results = await asyncio.gather(*tasks, return_exceptions=True)

            elapsed = time.perf_counter() - start

            run_bytes = 0
            for result in results:
                if isinstance(result, int):
                    run_bytes += result
                    successful_connections += 1
                total_connections += 1

            if run_bytes > 0 and elapsed > 0:
                mbps = run_bytes * 8 / elapsed / 1000000

                speeds.append(mbps)

    return SpeedTestResult(
        median_speed_mbps=statistics.median(speeds),
        run_speeds_mbps=speeds,
        timestamp=datetime.now(),
        succesful_connections=successful_connections,
        total_connections=total_connections,
        route= "direct" if proxy is None else "proxy"
    )