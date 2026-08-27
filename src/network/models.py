from dataclasses import dataclass
from datetime import datetime
from typing import Literal


@dataclass
class ProbeResult:
    endpoint: str
    route: Literal["direct", "proxy"]
    method: Literal["icmp", "http"]
    timestamp: datetime
    reachable: bool
    latency_ms: float | None
    error: str | None = None


@dataclass
class SpeedTestResult:
    median_speed_mbps: float
    run_speeds_mbps: list[float]
    route: Literal["direct", "proxy"]
    timestamp: datetime
    succesful_connections: int
    total_connections: int