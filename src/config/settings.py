"""
Stores default values for the application.
"""


# network.probes default values
DEFAULT_PING_TIMEOUT = 5        
DEFAULT_PING_PROXY = None       # proxy

# network.speed default values
DEFAULT_SPEEDTEST_TIMEOUT = 30                                            
DEFAULT_SPEEDTEST_RUNS = 2                                                # number of sequential runs
DEFAULT_SPEEDTEST_CONNECTIONS = 3                                         # number of parallel connections in each run
DEFAULT_SPEEDTEST_PROXY = None
DEFAULT_SPEEDTEST_DOWNLOAD_BYTES = 5 * 1024 * 1024      # maximum number of bytes to download in each connection