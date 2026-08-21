import asyncio
import statistics
import time
from datetime import datetime

import httpx
from models import SpeedTestResult  # ty: ignore[unresolved-import]


async def _download(client: httpx.AsyncClient, url: str, max_bytes) -> int:
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
    runs: int = 2,
    connections: int = 2,
    bytes_per_connection: int = 5 * 1024 * 1024,
    proxy: str | None = None,
    timeout: float = 30,
) -> SpeedTestResult:
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
                _download(client, url=url, max_bytes=bytes_per_connection)
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
        download_avg_mbps=statistics.median(speeds),
        run_speeds_mbps=speeds,
        timestamp=datetime.now(),
        succesful_connections=successful_connections,
    )