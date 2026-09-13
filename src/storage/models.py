from dataclasses import dataclass
from network.models import ProbeResult, SpeedTestResult


@dataclass
class PersistentProbeResult:
    id: int
    checkpoint_id: int
    result: ProbeResult
    proxy_id: int | None


@dataclass
class PersistentSpeedTestResult:
    id: int
    config_id: int
    result: SpeedTestResult
    proxy_id: int | None