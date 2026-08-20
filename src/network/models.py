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


