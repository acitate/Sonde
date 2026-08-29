from pathlib import Path
from dataclasses import dataclass


@dataclass
class Config:
    ping_timeout: float
    ping_proxy: str | None

    speedtest_timeout: float
    speedtest_runs: int
    speedtest_connections: int
    speedtest_proxy: str | None
    speedtest_download_bytes: int

    database_path: str | Path
