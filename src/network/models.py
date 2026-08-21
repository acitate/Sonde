from dataclasses import dataclass
from datetime import datetime
from typing import Literal


@dataclass
class ProbeResult:
    endpoint: str
    route: Literal["direct", "proxy"]
    timestamp: datetime
    reachable: bool
    latency: float | None
    method: Literal["icmp", "http"]


@dataclass
class SpeedTestResult:
    download_avg_mbps: float
    run_speeds_mbps: list[float]
    timestamp: datetime
    succesful_connections: int