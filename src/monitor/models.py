from dataclasses import dataclass
from typing import Literal
from collections.abc import Awaitable, Callable


@dataclass
class Proxy:
    id: int | None
    url: str


@dataclass
class LatencyCheckpoint:
    id: int | None
    endpoint: str
    method: Literal["icmp", "http"]
    timeout: float
    proxy_id: int | None
    enabled: bool


@dataclass
class SpeedTestConfig:
    id: int | None
    urls: list[str]
    runs: int
    connections: int
    bytes_per_connection: int
    timeout: float
    proxy_id: int | None
    active: bool


@dataclass
class ScheduledJob:
    name: str
    interval: float
    callback: Callable[[], Awaitable[None]]
