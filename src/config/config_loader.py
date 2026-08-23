from .models import Config
from .settings import *

# Parses config.toml if there is one and overloads the default values


def load_config() -> Config:
    config = Config()

    config.ping_proxy = DEFAULT_PING_PROXY
    config.ping_timeout = DEFAULT_PING_TIMEOUT

    config.speedtest_connections = DEFAULT_SPEEDTEST_CONNECTIONS
    config.speedtest_proxy = DEFAULT_SPEEDTEST_PROXY
    config.speedtest_download_bytes = DEFAULT_SPEEDTEST_DOWNLOAD_BYTES
    config.speedtest_runs = DEFAULT_SPEEDTEST_RUNS
    config.speedtest_timeout = DEFAULT_SPEEDTEST_TIMEOUT

    return config
