import asyncio
import logging
from dataclasses import replace
from datetime import datetime, timezone

from network.probes import endpoint_probe_http, endpoint_probe_icmp
from storage.repositories import Repository


logger = logging.getLogger(__name__)


class Monitor:
    def __init__(self, repository: Repository):
        self.repository = repository

    async def run_latency_checks(self) -> None:
        checkpoints = self.repository.get_enabled_latency_checkpoints()

        for checkpoint in checkpoints:
            try:
                await self._run_latency_check(checkpoint)

            except Exception:
                logger.exception(
                    "Latency check failed for checkpoint %s",
                    checkpoint.id,
                )

    async def _run_latency_check(self, checkpoint) -> None:
        if checkpoint.id is None:
            raise ValueError("Checkpoint ID cannot be None")

        proxy_url = None

        if checkpoint.proxy_id is not None:
            proxy = self.repository.get_proxy(checkpoint.proxy_id)

            if proxy is None:
                raise ValueError(
                    f"Proxy {checkpoint.proxy_id} was not found"
                )

            proxy_url = proxy.url

        if checkpoint.method == "icmp":
            result = await asyncio.to_thread(
                endpoint_probe_icmp,
                checkpoint.endpoint,
                checkpoint.timeout,
            )

        elif checkpoint.method == "http":
            result = await asyncio.to_thread(
                endpoint_probe_http,
                checkpoint.endpoint,
                proxy_url,
                checkpoint.timeout,
            )

        else:
            raise ValueError(
                f"Unsupported probe method: {checkpoint.method}"
            )

        result = replace(
            result,
            timestamp=datetime.now(timezone.utc),
        )

        self.repository.save_latency_probe(
            checkpoint.id,
            result,
            checkpoint.proxy_id,
        )

        logger.info(
            "Latency check completed: checkpoint=%s, reachable=%s",
            checkpoint.id,
            result.reachable,
        )
