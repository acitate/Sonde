import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass


logger = logging.getLogger(__name__)


@dataclass
class ScheduledJob:
    name: str
    interval: float
    callback: Callable[[], Awaitable[None]]


class Scheduler:
    def __init__(self):
        self.jobs: list[ScheduledJob] = []
        self._stop_event = asyncio.Event()
        self._running = False

    def add_job(
        self,
        name: str,
        interval: float,
        callback: Callable[[], Awaitable[None]],
    ) -> None:
        if interval <= 0:
            raise ValueError("Job interval must be positive")

        if any(job.name == name for job in self.jobs):
            raise ValueError(f"Job '{name}' already exists")

        self.jobs.append(
            ScheduledJob(
                name=name,
                interval=interval,
                callback=callback,
            )
        )

    async def _run_job(self, job: ScheduledJob) -> None:
        while not self._stop_event.is_set():
            try:
                await job.callback()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Job '%s' failed", job.name)

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=job.interval,
                )
            except TimeoutError:
                pass

    async def run(self) -> None:
        if self._running:
            raise RuntimeError("Scheduler is already running")

        self._running = True

        try:
            tasks = [
                asyncio.create_task(
                    self._run_job(job),
                    name=job.name,
                )
                for job in self.jobs
            ]

            await asyncio.gather(*tasks)
        finally:
            self._running = False

    def stop(self) -> None:
        self._stop_event.set()
