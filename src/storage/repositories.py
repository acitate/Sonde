"""SQLite repository for proxies, latency checkpoints/probes and speed tests.

Conventions
-----------
* Every write runs inside ``Repository._transaction()``. Standalone writes
  commit on success and roll back on error; writes inside a caller-owned
  transaction use a savepoint and leave the caller's transaction open.
  Constraint failures surface as ``ConstraintError`` subclasses instead of raw
  ``sqlite3.IntegrityError``.
* Timestamps are stored as fixed-width UTC ISO-8601 strings and returned as
  timezone-aware UTC ``datetime`` objects. Naive datetimes are rejected.
* A ``Repository`` wraps one connection. SQLite connections are not safe to
  share between threads, so create one ``Repository`` (and connection) per
  thread, or funnel all writes through a single writer thread.
"""

import sqlite3
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Literal, NamedTuple

from monitor.models import LatencyCheckpoint, Proxy, SpeedTestConfig
from network.models import ProbeResult, SpeedTestResult
from storage.models import PersistentProbeResult, PersistentSpeedTestResult

Method = Literal["icmp", "http"]
Route = Literal["direct", "proxy"]


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #


class RepositoryError(Exception):
    """Base class for repository errors."""


class ConstraintError(RepositoryError):
    """A database constraint (CHECK, FOREIGN KEY, UNIQUE, ...) was violated."""


class DuplicateError(ConstraintError):
    """A UNIQUE constraint was violated (e.g. proxy URL already exists)."""


class InUseError(ConstraintError):
    """The row cannot be deleted because other rows still reference it."""


def _translate_integrity_error(exc: sqlite3.IntegrityError) -> ConstraintError:
    message = str(exc)
    if "UNIQUE constraint failed" in message:
        return DuplicateError(message)
    return ConstraintError(message)


# --------------------------------------------------------------------------- #
# Return types (tuple-compatible with the previous (id, ..., ...) tuples)
# --------------------------------------------------------------------------- #


class ProbeRecord(NamedTuple):
    """A stored probe together with its checkpoint as it is *now*.

    ``result.result.endpoint`` is the endpoint snapshot taken when the probe ran;
    ``checkpoint.endpoint`` is the checkpoint's current endpoint. They differ if
    the checkpoint was edited after the probe.
    """

    id: int
    checkpoint: LatencyCheckpoint
    result: PersistentProbeResult


class SpeedTestRecord(NamedTuple):
    """A stored speed test together with its config."""

    id: int
    config: SpeedTestConfig
    result: PersistentSpeedTestResult


# --------------------------------------------------------------------------- #
# Sentinel for "argument not provided" (so None can mean "set to NULL")
# --------------------------------------------------------------------------- #


class _Unset:
    def __repr__(self) -> str:
        return "UNSET"


UNSET = _Unset()


# --------------------------------------------------------------------------- #
# Time helpers
# --------------------------------------------------------------------------- #


def _to_db(value: datetime) -> str:
    """Serialize an aware datetime as a fixed-width UTC string.

    Fixed-width, single-offset strings sort lexicographically in chronological
    order, which is what the range/ordering queries rely on.
    """
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware (naive datetime given)")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _from_db(value: str) -> datetime:
    return datetime.fromisoformat(value)


# --------------------------------------------------------------------------- #
# SQL fragments (one definition of every column list)
# --------------------------------------------------------------------------- #


def _select(alias: str, prefix: str, columns: Sequence[str]) -> str:
    return ", ".join(f"{alias}.{col} AS {prefix}{col}" for col in columns)


_PROXY_COLS = ("id", "url")
_CHECKPOINT_COLS = ("id", "endpoint", "method", "timeout", "proxy_id", "enabled")
_PROBE_COLS = (
    "id",
    "checkpoint_id",
    "endpoint",
    "route",
    "method",
    "timestamp",
    "reachable",
    "latency_ms",
    "error",
    "proxy_id",
)
_CONFIG_COLS = (
    "id",
    "runs",
    "connections",
    "bytes_per_connection",
    "timeout",
    "proxy_id",
    "active",
)
_SPEED_TEST_COLS = (
    "id",
    "config_id",
    "timestamp",
    "route",
    "proxy_id",
    "median_speed_mbps",
    "successful_connections",
    "total_connections",
)

_PROXY_SELECT = f"SELECT {_select('p', '', _PROXY_COLS)} FROM proxies p"
_CHECKPOINT_SELECT = (
    f"SELECT {_select('lc', '', _CHECKPOINT_COLS)} FROM latency_checkpoints lc"
)
_CONFIG_SELECT = f"SELECT {_select('c', '', _CONFIG_COLS)} FROM speed_test_configs c"

_PROBE_SELECT = f"""
    SELECT {_select('lp', 'lp_', _PROBE_COLS)},
           {_select('lc', 'lc_', _CHECKPOINT_COLS)}
    FROM latency_probes lp
    JOIN latency_checkpoints lc ON lc.id = lp.checkpoint_id
"""
_SPEED_TEST_SELECT = f"""
    SELECT {_select('st', 'st_', _SPEED_TEST_COLS)},
           {_select('stc', 'stc_', _CONFIG_COLS)}
    FROM speed_tests st
    JOIN speed_test_configs stc ON stc.id = st.config_id
"""

# Stay well under SQLITE_MAX_VARIABLE_NUMBER (999 on older SQLite builds).
_IN_CHUNK = 500


def _chunked(items: Sequence[int], size: int = _IN_CHUNK) -> Iterator[Sequence[int]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _validate_limit(limit: int) -> int:
    # SQLite treats a negative LIMIT as "no limit"; refuse it explicitly.
    if limit < 1:
        raise ValueError("limit must be >= 1")
    return limit


def _range_bounds(start_time: datetime, end_time: datetime) -> tuple[str, str]:
    """Serialize range bounds (rejecting naive datetimes) and check their order."""
    start, end = _to_db(start_time), _to_db(end_time)
    if start > end:  # fixed-width UTC strings compare chronologically
        raise ValueError("start_time must not be after end_time")
    return start, end


# --------------------------------------------------------------------------- #
# Row -> domain object mappers
# --------------------------------------------------------------------------- #


def _proxy_from_row(row: sqlite3.Row, prefix: str = "") -> Proxy:
    return Proxy(id=row[f"{prefix}id"], url=row[f"{prefix}url"])


def _checkpoint_from_row(row: sqlite3.Row, prefix: str = "") -> LatencyCheckpoint:
    return LatencyCheckpoint(
        id=row[f"{prefix}id"],
        endpoint=row[f"{prefix}endpoint"],
        method=row[f"{prefix}method"],
        timeout=row[f"{prefix}timeout"],
        proxy_id=row[f"{prefix}proxy_id"],
        enabled=bool(row[f"{prefix}enabled"]),
    )


def _config_from_row(
    row: sqlite3.Row, urls: list[str], prefix: str = ""
) -> SpeedTestConfig:
    return SpeedTestConfig(
        id=row[f"{prefix}id"],
        urls=urls,
        runs=row[f"{prefix}runs"],
        connections=row[f"{prefix}connections"],
        bytes_per_connection=row[f"{prefix}bytes_per_connection"],
        timeout=row[f"{prefix}timeout"],
        proxy_id=row[f"{prefix}proxy_id"],
        active=bool(row[f"{prefix}active"]),
    )


def _probe_from_row(row: sqlite3.Row) -> PersistentProbeResult:
    return PersistentProbeResult(
        id=row["lp_id"],
        checkpoint_id=row["lp_checkpoint_id"],
        result=ProbeResult(
            endpoint=row["lp_endpoint"],
            route=row["lp_route"],
            method=row["lp_method"],
            timestamp=_from_db(row["lp_timestamp"]),
            reachable=bool(row["lp_reachable"]),
            latency_ms=row["lp_latency_ms"],
            error=row["lp_error"],
        ),
        proxy_id=row["lp_proxy_id"],
    )


def _speed_test_from_row(
    row: sqlite3.Row, run_speeds: list[float]
) -> PersistentSpeedTestResult:
    return PersistentSpeedTestResult(
        id=row["st_id"],
        config_id=row["st_config_id"],
        result=SpeedTestResult(
            median_speed_mbps=row["st_median_speed_mbps"],
            run_speeds_mbps=run_speeds,
            route=row["st_route"],
            timestamp=_from_db(row["st_timestamp"]),
            successful_connections=row["st_successful_connections"],
            total_connections=row["st_total_connections"],
        ),
        proxy_id=row["st_proxy_id"],
    )


# --------------------------------------------------------------------------- #
# Repository
# --------------------------------------------------------------------------- #


class Repository:
    """Data access for the application database.

    Args:
        conn: An open SQLite connection. Foreign key enforcement is switched on
            for it. Rows are read through a ``sqlite3.Row`` cursor, so the
            connection's own ``row_factory`` does not matter.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._savepoint_number = 0
        # No-op inside a transaction, so verify instead of assuming.
        self._conn.execute("PRAGMA foreign_keys = ON")
        if self._conn.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
            raise RepositoryError(
                "could not enable foreign key enforcement "
                "(is a transaction already open on this connection?)"
            )

    # ----- internals ------------------------------------------------------- #

    def _execute(self, sql: str, params: Sequence = ()) -> sqlite3.Cursor:
        cursor = self._conn.cursor()
        cursor.row_factory = sqlite3.Row
        return cursor.execute(sql, params)

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        """Own a standalone transaction or isolate work with a savepoint.

        A repository owns the transaction only when the connection is not
        already in one. For a caller-owned transaction, this context manager
        releases or rolls back only its savepoint and never commits or rolls
        back the caller's transaction.
        """
        if not self._conn.in_transaction:
            try:
                with self._conn:
                    yield
            except sqlite3.IntegrityError as exc:
                raise _translate_integrity_error(exc) from exc
            return

        savepoint = f"repository_{id(self):x}_{self._savepoint_number}"
        self._savepoint_number += 1
        self._conn.execute(f"SAVEPOINT {savepoint}")
        try:
            yield
        except BaseException as exc:
            self._conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            self._conn.execute(f"RELEASE SAVEPOINT {savepoint}")
            if isinstance(exc, sqlite3.IntegrityError):
                raise _translate_integrity_error(exc) from exc
            raise
        else:
            self._conn.execute(f"RELEASE SAVEPOINT {savepoint}")

    def _urls_for_configs(self, config_ids: Iterable[int]) -> dict[int, list[str]]:
        """URLs (in position order) for several configs in few queries."""
        ids = sorted(set(config_ids))
        urls: dict[int, list[str]] = {config_id: [] for config_id in ids}
        for chunk in _chunked(ids):
            marks = ",".join("?" * len(chunk))
            rows = self._execute(
                f"""
                SELECT config_id, url FROM speed_test_urls
                WHERE config_id IN ({marks})
                ORDER BY config_id, position
                """,
                chunk,
            )
            for row in rows:
                urls[row["config_id"]].append(row["url"])
        return urls

    def _run_speeds_for_tests(self, test_ids: Iterable[int]) -> dict[int, list[float]]:
        """Per-run speeds (in run order) for several speed tests in few queries."""
        ids = sorted(set(test_ids))
        speeds: dict[int, list[float]] = {test_id: [] for test_id in ids}
        for chunk in _chunked(ids):
            marks = ",".join("?" * len(chunk))
            rows = self._execute(
                f"""
                SELECT speed_test_id, speed_mbps FROM speed_test_runs
                WHERE speed_test_id IN ({marks})
                ORDER BY speed_test_id, run_number
                """,
                chunk,
            )
            for row in rows:
                speeds[row["speed_test_id"]].append(row["speed_mbps"])
        return speeds

    def _query_probes(
        self, where: str = "", params: Sequence = (), tail: str = ""
    ) -> list[ProbeRecord]:
        rows = self._execute(f"{_PROBE_SELECT} {where} {tail}", params).fetchall()
        return [
            ProbeRecord(
                id=row["lp_id"],
                checkpoint=_checkpoint_from_row(row, "lc_"),
                result=_probe_from_row(row),
            )
            for row in rows
        ]

    def _query_speed_tests(
        self, where: str = "", params: Sequence = (), tail: str = ""
    ) -> list[SpeedTestRecord]:
        rows = self._execute(f"{_SPEED_TEST_SELECT} {where} {tail}", params).fetchall()
        urls = self._urls_for_configs(row["st_config_id"] for row in rows)
        runs = self._run_speeds_for_tests(row["st_id"] for row in rows)
        return [
            SpeedTestRecord(
                id=row["st_id"],
                # Fresh list per record so callers can't mutate each other's data.
                config=_config_from_row(row, list(urls[row["st_config_id"]]), "stc_"),
                result=_speed_test_from_row(row, runs[row["st_id"]]),
            )
            for row in rows
        ]

    # ----- proxies --------------------------------------------------------- #

    def create_proxy(self, url: str) -> Proxy:
        """Create a proxy.

        Raises:
            DuplicateError: If a proxy with this URL already exists.
        """
        with self._transaction():
            proxy_id = self._execute(
                "INSERT INTO proxies (url) VALUES (?)", (url,)
            ).lastrowid
        return Proxy(id=proxy_id, url=url)

    def get_proxy(self, proxy_id: int) -> Proxy | None:
        row = self._execute(f"{_PROXY_SELECT} WHERE p.id = ?", (proxy_id,)).fetchone()
        return _proxy_from_row(row) if row else None

    def get_proxy_by_url(self, url: str) -> Proxy | None:
        row = self._execute(f"{_PROXY_SELECT} WHERE p.url = ?", (url,)).fetchone()
        return _proxy_from_row(row) if row else None

    def get_all_proxies(self) -> list[Proxy]:
        rows = self._execute(f"{_PROXY_SELECT} ORDER BY p.id").fetchall()
        return [_proxy_from_row(row) for row in rows]

    def get_or_create_proxy(self, url: str) -> Proxy:
        """Return the proxy with this URL, creating it if necessary."""
        proxy = self.get_proxy_by_url(url)
        if proxy is not None:
            return proxy
        try:
            return self.create_proxy(url)
        except DuplicateError:
            # Created by another connection between our read and write.
            proxy = self.get_proxy_by_url(url)
            if proxy is None:
                raise
            return proxy

    def update_proxy(self, proxy_id: int, url: str) -> bool:
        """Change a proxy's URL. Returns False if the proxy doesn't exist.

        Raises:
            DuplicateError: If another proxy already uses this URL.
        """
        with self._transaction():
            cursor = self._execute(
                "UPDATE proxies SET url = ? WHERE id = ?", (url, proxy_id)
            )
        return cursor.rowcount == 1

    def delete_proxy(self, proxy_id: int) -> bool:
        """Delete a proxy. Returns False if it doesn't exist.

        Raises:
            InUseError: If a checkpoint, speed config, probe or speed test still
                references the proxy.
        """
        try:
            with self._transaction():
                cursor = self._execute("DELETE FROM proxies WHERE id = ?", (proxy_id,))
        except ConstraintError as exc:
            raise InUseError(f"proxy {proxy_id} is still referenced") from exc
        return cursor.rowcount == 1

    # ----- latency checkpoints --------------------------------------------- #

    def create_latency_checkpoint(
        self,
        endpoint: str,
        method: Method,
        timeout: float,
        proxy_id: int | None = None,
        enabled: bool = True,
    ) -> LatencyCheckpoint:
        """Create a checkpoint.

        Raises:
            ConstraintError: On an invalid method/timeout, unknown proxy, or an
                ICMP checkpoint with a proxy.
        """
        with self._transaction():
            checkpoint_id = self._execute(
                """
                INSERT INTO latency_checkpoints
                    (endpoint, method, timeout, proxy_id, enabled)
                VALUES (?, ?, ?, ?, ?)
                """,
                (endpoint, method, timeout, proxy_id, int(enabled)),
            ).lastrowid
        return LatencyCheckpoint(
            id=checkpoint_id,
            endpoint=endpoint,
            method=method,
            timeout=timeout,
            proxy_id=proxy_id,
            enabled=enabled,
        )

    def get_latency_checkpoint(self, checkpoint_id: int) -> LatencyCheckpoint | None:
        row = self._execute(
            f"{_CHECKPOINT_SELECT} WHERE lc.id = ?", (checkpoint_id,)
        ).fetchone()
        return _checkpoint_from_row(row) if row else None

    def get_all_latency_checkpoints(self) -> list[LatencyCheckpoint]:
        rows = self._execute(f"{_CHECKPOINT_SELECT} ORDER BY lc.id").fetchall()
        return [_checkpoint_from_row(row) for row in rows]

    def get_enabled_latency_checkpoints(self) -> list[LatencyCheckpoint]:
        rows = self._execute(
            f"{_CHECKPOINT_SELECT} WHERE lc.enabled = 1 ORDER BY lc.id"
        ).fetchall()
        return [_checkpoint_from_row(row) for row in rows]

    def update_latency_checkpoint(
        self,
        checkpoint_id: int,
        endpoint: str | _Unset = UNSET,
        method: Method | _Unset = UNSET,
        timeout: float | _Unset = UNSET,
        proxy_id: int | None | _Unset = UNSET,
        enabled: bool | _Unset = UNSET,
    ) -> bool:
        """Update the given fields of a checkpoint.

        Arguments left at ``UNSET`` are not touched. ``proxy_id=None`` clears the
        proxy (switch to direct); it is *not* the same as leaving it unset.

        Returns:
            True if the checkpoint existed and was updated, False if it doesn't
            exist.

        Raises:
            ValueError: If no field was given.
            ConstraintError: If the result would violate a constraint (e.g.
                ``method='icmp'`` while a proxy is still set).
        """
        fields = {
            "endpoint": endpoint,
            "method": method,
            "timeout": timeout,
            "proxy_id": proxy_id,
            "enabled": enabled if isinstance(enabled, _Unset) else int(enabled),
        }
        changes = {col: val for col, val in fields.items() if val is not UNSET}
        if not changes:
            raise ValueError("update_latency_checkpoint called with no fields")

        # Column names come from the fixed dict above, never from caller input;
        # all values are bound parameters.
        assignments = ", ".join(f"{col} = ?" for col in changes)
        with self._transaction():
            cursor = self._execute(
                f"UPDATE latency_checkpoints SET {assignments} WHERE id = ?",
                (*changes.values(), checkpoint_id),
            )
        return cursor.rowcount == 1

    def delete_latency_checkpoint(
        self, checkpoint_id: int, delete_history: bool = False
    ) -> bool:
        """Delete a checkpoint. Returns False if it doesn't exist.

        Args:
            checkpoint_id: Checkpoint to delete.
            delete_history: Also delete all of its probes. Without this, a
                checkpoint that has probes is refused.

        Raises:
            InUseError: If the checkpoint has probes and ``delete_history`` is False.
        """
        with self._transaction():
            probe_count = self._execute(
                "SELECT COUNT(*) FROM latency_probes WHERE checkpoint_id = ?",
                (checkpoint_id,),
            ).fetchone()[0]
            if probe_count:
                if not delete_history:
                    raise InUseError(
                        f"checkpoint {checkpoint_id} has {probe_count} probes; "
                        "pass delete_history=True to delete them too"
                    )
                self._execute(
                    "DELETE FROM latency_probes WHERE checkpoint_id = ?",
                    (checkpoint_id,),
                )
            cursor = self._execute(
                "DELETE FROM latency_checkpoints WHERE id = ?", (checkpoint_id,)
            )
        return cursor.rowcount == 1

    # ----- latency probes -------------------------------------------------- #

    def create_latency_probe(
        self,
        checkpoint_id: int,
        endpoint: str,
        route: Route,
        method: Method,
        timestamp: datetime,
        reachable: bool,
        latency_ms: float | None,
        error: str | None = None,
        proxy_id: int | None = None,
    ) -> int:
        """Store one probe and return its id.

        Raises:
            ValueError: If ``timestamp`` is naive.
            ConstraintError: If the row violates a constraint (unknown checkpoint,
                route/proxy mismatch, reachable without latency, ...).
        """
        with self._transaction():
            return self._execute(
                """
                INSERT INTO latency_probes
                    (checkpoint_id, endpoint, route, method, timestamp,
                     reachable, latency_ms, error, proxy_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    checkpoint_id,
                    endpoint,
                    route,
                    method,
                    _to_db(timestamp),
                    int(reachable),
                    latency_ms,
                    error,
                    proxy_id,
                ),
            ).lastrowid

    def save_latency_probe(
        self, checkpoint_id: int, result: ProbeResult, proxy_id: int | None = None
    ) -> int:
        """Store a ``ProbeResult`` for a checkpoint and return the probe id."""
        return self.create_latency_probe(
            checkpoint_id=checkpoint_id,
            endpoint=result.endpoint,
            route=result.route,
            method=result.method,
            timestamp=result.timestamp,
            reachable=result.reachable,
            latency_ms=result.latency_ms,
            error=result.error,
            proxy_id=proxy_id,
        )

    def get_latency_probe(self, probe_id: int) -> ProbeRecord | None:
        records = self._query_probes("WHERE lp.id = ?", (probe_id,))
        return records[0] if records else None

    def get_latest_latency_probes(
        self, limit: int = 100, checkpoint_id: int | None = None
    ) -> list[ProbeRecord]:
        """Most recent probes first, optionally for a single checkpoint."""
        _validate_limit(limit)
        where, params = "", []
        if checkpoint_id is not None:
            where, params = "WHERE lp.checkpoint_id = ?", [checkpoint_id]
        return self._query_probes(
            where,
            (*params, limit),
            "ORDER BY lp.timestamp DESC, lp.id DESC LIMIT ?",
        )

    def get_latest_probe_per_checkpoint(self) -> list[ProbeRecord]:
        """The newest probe of every checkpoint that has any, by checkpoint id."""
        return self._query_probes(
            """
            WHERE lp.id = (
                SELECT p2.id FROM latency_probes p2
                WHERE p2.checkpoint_id = lp.checkpoint_id
                ORDER BY p2.timestamp DESC, p2.id DESC
                LIMIT 1
            )
            """,
            (),
            "ORDER BY lp.checkpoint_id",
        )

    def get_range_latency_probes(
        self, checkpoint_id: int, start_time: datetime, end_time: datetime
    ) -> list[ProbeRecord]:
        """Probes of a checkpoint with ``start_time <= timestamp <= end_time``,
        oldest first."""
        start, end = _range_bounds(start_time, end_time)
        return self._query_probes(
            """
            WHERE lp.checkpoint_id = ?
              AND lp.timestamp >= ?
              AND lp.timestamp <= ?
            """,
            (checkpoint_id, start, end),
            "ORDER BY lp.timestamp, lp.id",
        )

    def delete_latency_probes_before(self, cutoff: datetime) -> int:
        """Delete probes older than ``cutoff`` (retention). Returns rows deleted."""
        with self._transaction():
            cursor = self._execute(
                "DELETE FROM latency_probes WHERE timestamp < ?", (_to_db(cutoff),)
            )
        return cursor.rowcount

    # ----- speed test configs ---------------------------------------------- #

    def create_speed_test_config(
        self,
        urls: list[str],
        runs: int,
        connections: int,
        bytes_per_connection: int,
        timeout: float,
        proxy_id: int | None = None,
        active: bool = False,
    ) -> SpeedTestConfig:
        """Create a config. With ``active=True`` it replaces the active config.

        Raises:
            ValueError: If ``urls`` is empty or contains duplicates.
            ConstraintError: On invalid numbers or an unknown proxy.
        """
        if not urls:
            raise ValueError("a speed test config needs at least one URL")
        if len(set(urls)) != len(urls):
            raise ValueError("speed test URLs must be unique within a config")

        with self._transaction():
            if active:
                self._execute("UPDATE speed_test_configs SET active = 0 WHERE active = 1")
            config_id = self._execute(
                """
                INSERT INTO speed_test_configs
                    (runs, connections, bytes_per_connection, timeout, proxy_id, active)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (runs, connections, bytes_per_connection, timeout, proxy_id, int(active)),
            ).lastrowid
            self._conn.executemany(
                "INSERT INTO speed_test_urls (config_id, url, position) VALUES (?, ?, ?)",
                [(config_id, url, position) for position, url in enumerate(urls)],
            )
        return SpeedTestConfig(
            id=config_id,
            urls=list(urls),
            runs=runs,
            connections=connections,
            bytes_per_connection=bytes_per_connection,
            timeout=timeout,
            proxy_id=proxy_id,
            active=active,
        )

    def get_speed_test_config(self, config_id: int) -> SpeedTestConfig | None:
        row = self._execute(f"{_CONFIG_SELECT} WHERE c.id = ?", (config_id,)).fetchone()
        if row is None:
            return None
        return _config_from_row(row, self._urls_for_configs([config_id])[config_id])

    def get_all_speed_test_configs(self) -> list[SpeedTestConfig]:
        rows = self._execute(f"{_CONFIG_SELECT} ORDER BY c.id").fetchall()
        urls = self._urls_for_configs(row["id"] for row in rows)
        return [_config_from_row(row, urls[row["id"]]) for row in rows]

    def get_active_speed_test_config(self) -> SpeedTestConfig | None:
        row = self._execute(f"{_CONFIG_SELECT} WHERE c.active = 1").fetchone()
        if row is None:
            return None
        return _config_from_row(row, self._urls_for_configs([row["id"]])[row["id"]])

    def set_active_speed_test_config(self, config_id: int) -> bool:
        """Make a config the single active one.

        Returns False (and changes nothing) if the config doesn't exist.
        """
        with self._transaction():
            if self._execute(
                "SELECT 1 FROM speed_test_configs WHERE id = ?", (config_id,)
            ).fetchone() is None:
                return False
            self._execute("UPDATE speed_test_configs SET active = 0 WHERE active = 1")
            self._execute(
                "UPDATE speed_test_configs SET active = 1 WHERE id = ?", (config_id,)
            )
        return True

    def delete_speed_test_config(
        self, config_id: int, delete_history: bool = False
    ) -> bool:
        """Delete a config and its URLs. Returns False if it doesn't exist.

        Args:
            config_id: Config to delete.
            delete_history: Also delete all speed tests (and their runs) recorded
                with this config. Without this, a config with results is refused.

        Raises:
            InUseError: If the config has results and ``delete_history`` is False.
        """
        with self._transaction():
            test_count = self._execute(
                "SELECT COUNT(*) FROM speed_tests WHERE config_id = ?", (config_id,)
            ).fetchone()[0]
            if test_count:
                if not delete_history:
                    raise InUseError(
                        f"speed test config {config_id} has {test_count} results; "
                        "pass delete_history=True to delete them too"
                    )
                # speed_test_runs rows go with them (ON DELETE CASCADE).
                self._execute(
                    "DELETE FROM speed_tests WHERE config_id = ?", (config_id,)
                )
            cursor = self._execute(
                "DELETE FROM speed_test_configs WHERE id = ?", (config_id,)
            )
        return cursor.rowcount == 1

    # ----- speed test results ---------------------------------------------- #

    def save_speed_test_result(
        self,
        config_id: int,
        timestamp: datetime,
        route: Route,
        median_speed_mbps: float,
        successful_connections: int,
        total_connections: int,
        proxy_id: int | None = None,
        run_speeds: list[float] | None = None,
    ) -> int:
        """Store a speed test (and its per-run speeds) and return its id.

        Raises:
            ValueError: If ``timestamp`` is naive, or if ``run_speeds`` is not
                ``None`` and its length does not match the referenced config's
                ``runs`` value.
            ConstraintError: If the row violates a constraint.
        """
        with self._transaction():
            config_row = self._execute(
                "SELECT runs FROM speed_test_configs WHERE id = ?", (config_id,)
            ).fetchone()
            if config_row is not None and run_speeds is not None:
                expected_runs = config_row["runs"]
                if len(run_speeds) != expected_runs:
                    raise ValueError(
                        "run_speeds length must match the config's runs value "
                        f"({expected_runs})"
                    )

            speed_test_id = self._execute(
                """
                INSERT INTO speed_tests
                    (config_id, timestamp, route, proxy_id, median_speed_mbps,
                     successful_connections, total_connections)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    config_id,
                    _to_db(timestamp),
                    route,
                    proxy_id,
                    median_speed_mbps,
                    successful_connections,
                    total_connections,
                ),
            ).lastrowid
            if run_speeds:
                self._conn.executemany(
                    """
                    INSERT INTO speed_test_runs (speed_test_id, run_number, speed_mbps)
                    VALUES (?, ?, ?)
                    """,
                    [
                        (speed_test_id, run_number, speed)
                        for run_number, speed in enumerate(run_speeds, start=1)
                    ],
                )
        return speed_test_id

    def save_speed_test(
        self, config_id: int, result: SpeedTestResult, proxy_id: int | None = None
    ) -> int:
        """Store a ``SpeedTestResult`` for a config and return the speed test id."""
        return self.save_speed_test_result(
            config_id=config_id,
            timestamp=result.timestamp,
            route=result.route,
            median_speed_mbps=result.median_speed_mbps,
            successful_connections=result.successful_connections,
            total_connections=result.total_connections,
            proxy_id=proxy_id,
            run_speeds=result.run_speeds_mbps,
        )

    def get_speed_test_result(self, result_id: int) -> SpeedTestRecord | None:
        records = self._query_speed_tests("WHERE st.id = ?", (result_id,))
        return records[0] if records else None

    def get_latest_speed_test_results(
        self, limit: int = 10, config_id: int | None = None
    ) -> list[SpeedTestRecord]:
        """Most recent speed tests first, optionally for a single config."""
        _validate_limit(limit)
        where, params = "", []
        if config_id is not None:
            where, params = "WHERE st.config_id = ?", [config_id]
        return self._query_speed_tests(
            where,
            (*params, limit),
            "ORDER BY st.timestamp DESC, st.id DESC LIMIT ?",
        )

    def get_range_speed_test_results(
        self, config_id: int, start_time: datetime, end_time: datetime
    ) -> list[SpeedTestRecord]:
        """Speed tests of a config with ``start_time <= timestamp <= end_time``,
        oldest first."""
        start, end = _range_bounds(start_time, end_time)
        return self._query_speed_tests(
            """
            WHERE st.config_id = ?
              AND st.timestamp >= ?
              AND st.timestamp <= ?
            """,
            (config_id, start, end),
            "ORDER BY st.timestamp, st.id",
        )

    def delete_speed_tests_before(self, cutoff: datetime) -> int:
        """Delete speed tests (and their runs) older than ``cutoff``.

        Returns the number of speed tests deleted.
        """
        with self._transaction():
            cursor = self._execute(
                "DELETE FROM speed_tests WHERE timestamp < ?", (_to_db(cutoff),)
            )
        return cursor.rowcount
