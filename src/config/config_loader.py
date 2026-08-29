from .models import Config
from .settings import *

# Parses config.toml if there is one and overloads the default values


def load_config() -> Config:
    config = Config(
        ping_proxy=DEFAULT_PING_PROXY,
        ping_timeout=DEFAULT_PING_TIMEOUT,
        speedtest_connections=DEFAULT_SPEEDTEST_CONNECTIONS,
        speedtest_proxy=DEFAULT_SPEEDTEST_PROXY,
        speedtest_download_bytes=DEFAULT_SPEEDTEST_DOWNLOAD_BYTES,
        speedtest_runs=DEFAULT_SPEEDTEST_RUNS,
        speedtest_timeout=DEFAULT_SPEEDTEST_TIMEOUT,
        database_path=DEFAULT_DB_PATH,
    )

    return config
