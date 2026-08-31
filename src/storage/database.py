import sqlite3
from pathlib import Path


def create_database(path: str | Path) -> sqlite3.Connection:
    """Create the SQLite database and parent directories if necessary.

    Args:
        path (str | Path): Path to the database file.

    Returns:
        sqlite3.Connection: Connection to the created database.
    """
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    return sqlite3.connect(db_path)


def initialize_database(connection: sqlite3.Connection) -> None:
    """Initialize the application schema if it doesn't exist. 

    Args:
        connection ( sqlite3.Connection ): Connection to the application database.
    """
    connection.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS proxies (
            id INTEGER PRIMARY KEY,
            url TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS latency_checkpoints (
            id INTEGER PRIMARY KEY,
            endpoint TEXT NOT NULL,
            method TEXT NOT NULL
                CHECK (method IN ('http', 'icmp')),
            timeout REAL NOT NULL
                CHECK (timeout > 0),
            proxy_id INTEGER, 
            enabled INTEGER
                CHECK (enabled IN (0, 1)),
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

            FOREIGN KEY (proxy_id)
                REFERENCES proxies(id)
                ON DELETE RESTRICT,

            CHECK (
                (method = 'icmp' AND proxy_id IS NULL)
                OR
                method = 'http'
            )
        );

        CREATE TABLE IF NOT EXISTS latency_probes (
            id INTEGER PRIMARY KEY,
            checkpoint_id INTEGER NOT NULL,
            endpoint TEXT NOT NULL,
            route TEXT NOT NULL
                CHECK (route IN ('direct', 'proxy')),
            method TEXT NOT NULL
                CHECK (method in ('icmp', 'http')),
            timestamp TEXT NOT NULL,
            reachable INTEGER NOT NULL
                CHECK (reachable in (0, 1)),
            latency_ms REAL,
            error TEXT,
            proxy_id INTEGER,

            FOREIGN KEY (checkpoint_id)
                REFERENCES latency_checkpoints(id)
                ON DELETE RESTRICT,

            FOREIGN KEY (proxy_id)
                REFERENCES proxies(id)
                ON DELETE RESTRICT,

            CHECK (
                (route = 'direct' AND proxy_id IS NULL)
                OR
                (route = 'proxy' AND proxy_id IS NOT NULL)
            ),

            CHECK (
                reachable = 0
                OR
                latency_ms IS NOT NULL
            )
        );

        CREATE TABLE IF NOT EXISTS speed_test_configs (
            id INTEGER PRIMARY KEY,
            runs INTEGER NOT NULL
                CHECK (runs > 0),
            connections INTEGER NOT NULL
                CHECK (connections > 0),
            bytes_per_connection INTEGER NOT NULL
                CHECK (bytes_per_connection > 0),
            timeout REAL NOT NULL
                CHECK (timeout > 0),
            active INTEGER NOT NULL DEFAULT 0
                CHECK (active in (0, 1)),
            proxy_id INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (proxy_id)
                REFERENCES proxies(id)
                ON DELETE RESTRICT
        );

        CREATE UNIQUE INDEX IF NOT EXISTS one_active_speed_config
            ON speed_test_configs(active)
            WHERE active = 1;

        CREATE TABLE IF NOT EXISTS speed_test_urls (
            id INTEGER PRIMARY KEY,
            config_id INTEGER NOT NULL,
            url TEXT NOT NULL,
            position INTEGER NOT NULL
                CHECK (position >= 0),

            FOREIGN KEY (config_id)
                REFERENCES speed_test_configs(id)
                ON DELETE CASCADE,

            UNIQUE (config_id, position),
            UNIQUE (config_id, url)
        );

        CREATE TABLE IF NOT EXISTS speed_tests (
            id INTEGER PRIMARY KEY,
            config_id INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            route TEXT NOT NULL
                CHECK (route IN ('direct', 'proxy')),
            proxy_id INTEGER,
            median_speed_mbps REAL NOT NULL
                CHECK (median_speed_mbps >= 0),
            successful_connections INTEGER NOT NULL
                CHECK (successful_connections >= 0),
            total_connections INTEGER NOT NULL
                CHECK (total_connections >= 0),

            FOREIGN KEY (config_id)
                REFERENCES speed_test_configs(id)
                ON DELETE RESTRICT,

            FOREIGN KEY (proxy_id)
                REFERENCES proxies(id)
                ON DELETE RESTRICT,

            CHECK (
                (route = 'direct' AND proxy_id IS NULL)
                OR
                (route = 'proxy' AND proxy_id IS NOT NULL)
            ),

            CHECK (
                successful_connections <= total_connections
            )
        );

        CREATE TABLE IF NOT EXISTS speed_test_runs (
            id INTEGER PRIMARY KEY,
            speed_test_id INTEGER NOT NULL,
            run_number INTEGER NOT NULL
                CHECK (run_number > 0),
            speed_mbps REAL NOT NULL
                CHECK (speed_mbps >= 0),

            FOREIGN KEY (speed_test_id)
                REFERENCES speed_tests(id)
                ON DELETE CASCADE,

            UNIQUE (speed_test_id, run_number)
        );

        CREATE INDEX IF NOT EXISTS idx_latency_probes_checkpoint_timestamp
            ON latency_probes(checkpoint_id, timestamp);

        CREATE INDEX IF NOT EXISTS idx_latency_probes_timestamp
            ON latency_probes(timestamp);

        CREATE INDEX IF NOT EXISTS idx_speed_tests_config_timestamp
            ON speed_tests(config_id, timestamp);

        CREATE INDEX IF NOT EXISTS idx_speed_test_runs_test
            ON speed_test_runs(speed_test_id);
        """
    )

    connection.commit()
