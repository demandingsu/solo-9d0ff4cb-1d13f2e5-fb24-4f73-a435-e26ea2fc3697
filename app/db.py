"""SQLite persistence with transactional, versioned cell reports.

Every successful upload is recorded as a *batch*.  Samples remember which
batch created them (``samples.origin_batch_id``); batches remember every
sample they submitted (``sample_batches``).  Revoking a batch drops only
those samples that have no other active batch reference and were not
already present before batches existed (legacy rows keep a NULL origin).
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from contextlib import contextmanager
from typing import Iterable, List, Optional, Sequence, Tuple

from .algorithm import Cycle, detect_cycles

SCHEMA = """
CREATE TABLE IF NOT EXISTS cells (
    cell_id      TEXT PRIMARY KEY,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id     TEXT PRIMARY KEY,
    cell_id      TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'active',
    inserted     INTEGER NOT NULL DEFAULT 0,
    duplicate    INTEGER NOT NULL DEFAULT 0,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    revoked_at   TEXT
);

CREATE TABLE IF NOT EXISTS samples (
    cell_id         TEXT NOT NULL,
    ts              INTEGER NOT NULL,
    current_ma      INTEGER NOT NULL,
    origin_batch_id TEXT,
    PRIMARY KEY (cell_id, ts)
);

CREATE TABLE IF NOT EXISTS sample_batches (
    batch_id     TEXT NOT NULL,
    cell_id      TEXT NOT NULL,
    ts           INTEGER NOT NULL,
    PRIMARY KEY (batch_id, cell_id, ts),
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id)
);

CREATE INDEX IF NOT EXISTS idx_sample_batches_point
    ON sample_batches(cell_id, ts);
CREATE INDEX IF NOT EXISTS idx_batches_cell
    ON batches(cell_id);

CREATE TABLE IF NOT EXISTS report_versions (
    cell_id      TEXT NOT NULL,
    version      INTEGER NOT NULL,
    total_cycles INTEGER NOT NULL,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (cell_id, version)
);

CREATE TABLE IF NOT EXISTS cycles (
    cell_id       TEXT NOT NULL,
    version       INTEGER NOT NULL,
    cycle_no      INTEGER NOT NULL,
    charge_ah     REAL NOT NULL,
    discharge_ah  REAL NOT NULL,
    started_at    INTEGER NOT NULL,
    ended_at      INTEGER NOT NULL,
    PRIMARY KEY (cell_id, version, cycle_no)
);

CREATE TABLE IF NOT EXISTS upload_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    cell_id      TEXT NOT NULL,
    status       TEXT NOT NULL,
    detail       TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
"""


class ConflictError(Exception):
    """Same (cell, ts) already exists with a different current value."""

    def __init__(self, ts: int, existing: float, incoming: float):
        self.ts = ts
        self.existing = existing
        self.incoming = incoming
        super().__init__(
            f"conflict at ts={ts}: existing={existing}A incoming={incoming}A"
        )


class ImportError_(Exception):
    """Generic import failure (bad CSV payload etc.)."""


class UnknownBatchError(Exception):
    """No batch with the given id exists (nothing was changed)."""

    def __init__(self, batch_id: str):
        self.batch_id = batch_id
        super().__init__(f"unknown batch {batch_id}")


def _rollback(conn: sqlite3.Connection) -> None:
    """Roll back if a transaction is open; never mask the original error."""
    try:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        with self._conn() as conn:
            conn.executescript(SCHEMA)
            self._migrate(conn)

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        """Bring a pre-batch database up to date without touching its data.

        Existing sample rows keep ``origin_batch_id = NULL`` and are treated
        as legacy: they are never removed by a batch revocation.
        """
        columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(samples)")
        }
        if "origin_batch_id" not in columns:
            try:
                conn.execute("ALTER TABLE samples ADD COLUMN origin_batch_id TEXT")
            except sqlite3.OperationalError as exc:
                # Another process may have migrated it between our check and
                # the ALTER; only ignore that specific race.
                if "duplicate column" not in str(exc).lower():
                    raise

    @contextmanager
    def _conn(self):
        conn = _connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    # Import
    # ------------------------------------------------------------------ #
    def import_batch(
        self, cell_id: str, rows: Iterable[Tuple[int, float]]
    ) -> dict:
        """Atomically import a CSV batch and return its batch id.

        Returns a summary dict. Raises ConflictError / ImportError_ on
        rejection; in that case nothing is committed (no batch row, no
        samples, and the previous report stay untouched).
        """
        # Normalize + validate the payload before opening the transaction.
        incoming: list[tuple[int, float]] = []
        seen: dict[int, float] = {}
        try:
            for ts, current in rows:
                ts = int(ts)
                current = float(current)
                if ts < 0:
                    raise ValueError(f"negative timestamp {ts}")
                if not (current == current):  # NaN
                    raise ValueError("NaN current")
                if ts in seen and seen[ts] != current:
                    raise ImportError_(
                        f"batch itself contains conflicting values at ts={ts}"
                    )
                seen[ts] = current
                incoming.append((ts, current))
        except (TypeError, ValueError) as exc:
            raise ImportError_(f"invalid row: {exc}") from exc

        if not incoming:
            raise ImportError_("CSV contains no data rows")

        batch_id = uuid.uuid4().hex
        conn = _connect(self.db_path)
        try:
            # One writer at a time. BEGIN IMMEDIATE acquires the reserved
            # lock up front so concurrent uploads/revocations serialize
            # instead of hitting SQLITE_BUSY mid-transaction.
            conn.execute("BEGIN IMMEDIATE")

            # Reject whole batch on any (cell, ts) value mismatch before
            # writing anything.
            for ts, current in incoming:
                row = conn.execute(
                    "SELECT current_ma FROM samples WHERE cell_id=? AND ts=?",
                    (cell_id, ts),
                ).fetchone()
                if row is not None and row["current_ma"] != _to_storage(current):
                    raise ConflictError(ts, row["current_ma"] / 1_000_000.0, current)

            # Batch row first (sample_batches references it).
            conn.execute(
                "INSERT INTO batches(batch_id, cell_id, status)"
                " VALUES(?,?,'active')",
                (batch_id, cell_id),
            )

            inserted = 0
            duplicate = 0
            for ts, current in incoming:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO samples"
                    "(cell_id, ts, current_ma, origin_batch_id) VALUES(?,?,?,?)",
                    (cell_id, ts, _to_storage(current), batch_id),
                )
                if cur.rowcount:
                    inserted += 1
                else:
                    duplicate += 1

            # Record every submitted point, including all-duplicate ones.
            conn.executemany(
                "INSERT OR IGNORE INTO sample_batches(batch_id, cell_id, ts)"
                " VALUES(?,?,?)",
                [(batch_id, cell_id, ts) for ts, _ in incoming],
            )
            conn.execute(
                "UPDATE batches SET inserted=?, duplicate=? WHERE batch_id=?",
                (inserted, duplicate, batch_id),
            )
            self._touch_cell(conn, cell_id)

            # Recompute the report from the full committed sample set.
            cycles = self._detect_for_cell(conn, cell_id)
            version = self._append_report(conn, cell_id, cycles)

            conn.execute(
                "INSERT INTO upload_log(cell_id, status, detail) VALUES(?,?,?)",
                (
                    cell_id,
                    "accepted",
                    f"batch_id={batch_id} version={version}",
                ),
            )
            report = self._report_snapshot(conn, cell_id)
            conn.execute("COMMIT")
        except Exception:
            _rollback(conn)
            try:
                with self._conn() as log_conn:
                    log_conn.execute(
                        "INSERT INTO upload_log(cell_id, status, detail)"
                        " VALUES(?,?,?)",
                        (cell_id, "rejected", "batch rejected; no data changed"),
                    )
            except Exception:
                pass
            raise
        finally:
            conn.close()

        return {
            "cell_id": cell_id,
            "batch_id": batch_id,
            "inserted": inserted,
            "duplicate": duplicate,
            "version": version,
            "total_cycles": len(cycles),
        }

    # ------------------------------------------------------------------ #
    # Revocation
    # ------------------------------------------------------------------ #
    def revoke_batch(self, batch_id: str) -> dict:
        """Revoke one batch atomically.

        A sample survives while any *other* active batch references it, or
        when it predates batches (``origin_batch_id IS NULL``).  Only when
        the last basis is revoked is the point deleted, after which the
        cycles and their numbering are recomputed into a new report
        version.  A repeated revocation is a no-op and does not create a
        new version.  Raises UnknownBatchError without changing state.
        """
        conn = _connect(self.db_path)
        deleted = 0
        retained = 0
        cell_id = ""
        try:
            conn.execute("BEGIN IMMEDIATE")
            batch = conn.execute(
                "SELECT batch_id, cell_id, status FROM batches WHERE batch_id=?",
                (batch_id,),
            ).fetchone()
            if batch is None:
                raise UnknownBatchError(batch_id)

            cell_id = batch["cell_id"]

            if batch["status"] == "revoked":
                # Idempotent: never append a report version or alter data.
                report = self._report_snapshot(conn, cell_id)
                conn.execute("COMMIT")
                return {
                    "batch_id": batch_id,
                    "cell_id": cell_id,
                    "status": "revoked",
                    "already_revoked": True,
                    "deleted_samples": 0,
                    "retained_samples": 0,
                    **report,
                }

            points = [
                row["ts"]
                for row in conn.execute(
                    "SELECT ts FROM sample_batches WHERE batch_id=?"
                    " ORDER BY ts ASC",
                    (batch_id,),
                )
            ]

            conn.execute(
                "UPDATE batches SET status='revoked', revoked_at="
                "strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE batch_id=?",
                (batch_id,),
            )

            for ts in points:
                # Another batch still active for this point -> keep it.
                active = conn.execute(
                    "SELECT 1 FROM sample_batches sb"
                    " JOIN batches b ON b.batch_id=sb.batch_id"
                    " WHERE sb.cell_id=? AND sb.ts=? AND b.status='active'"
                    " LIMIT 1",
                    (cell_id, ts),
                ).fetchone()
                if active is not None:
                    retained += 1
                    continue

                sample = conn.execute(
                    "SELECT origin_batch_id FROM samples"
                    " WHERE cell_id=? AND ts=?",
                    (cell_id, ts),
                ).fetchone()
                if sample is None:
                    continue  # already absent, nothing to do
                if sample["origin_batch_id"] is None:
                    # Pre-batch legacy point: always keep it.
                    retained += 1
                    continue

                conn.execute(
                    "DELETE FROM samples WHERE cell_id=? AND ts=?",
                    (cell_id, ts),
                )
                deleted += 1

            if deleted:
                self._touch_cell(conn, cell_id)
                cycles = self._detect_for_cell(conn, cell_id)
                self._append_report(conn, cell_id, cycles)
                conn.execute(
                    "INSERT INTO upload_log(cell_id, status, detail) VALUES"
                    "(?,?,?)",
                    (
                        cell_id,
                        "revoked",
                        f"batch_id={batch_id} deleted={deleted}"
                        f" retained={retained}",
                    ),
                )

            report = self._report_snapshot(conn, cell_id)
            conn.execute("COMMIT")
        except UnknownBatchError:
            _rollback(conn)
            raise
        except Exception:
            _rollback(conn)
            raise
        finally:
            conn.close()

        return {
            "batch_id": batch_id,
            "cell_id": cell_id,
            "status": "revoked",
            "already_revoked": False,
            "deleted_samples": deleted,
            "retained_samples": retained,
            **report,
        }

    # ------------------------------------------------------------------ #
    # Queries
    # ------------------------------------------------------------------ #
    def get_report(self, cell_id: str) -> Optional[dict]:
        """Return report + cycles of the latest committed version.

        The version row, cycle details and total are all read from the same
        committed version; a single snapshot transaction guarantees they
        belong to one state even if an upload/revocation commits
        concurrently.
        """
        with self._conn() as conn:
            conn.execute("BEGIN")
            try:
                cell = conn.execute(
                    "SELECT cell_id FROM cells WHERE cell_id=?", (cell_id,)
                ).fetchone()
                if cell is None:
                    return None
                return self._report_snapshot(conn, cell_id)
            finally:
                conn.execute("COMMIT")

    def get_batch(self, batch_id: str) -> Optional[dict]:
        """Return batch metadata and point-reference counts in one snapshot."""
        with self._conn() as conn:
            conn.execute("BEGIN")
            try:
                row = conn.execute(
                    "SELECT batch_id, cell_id, status, inserted, duplicate,"
                    " created_at, revoked_at FROM batches WHERE batch_id=?",
                    (batch_id,),
                ).fetchone()
                if row is None:
                    return None
                out = dict(row)
                out["sample_refs"] = conn.execute(
                    "SELECT COUNT(*) AS n FROM sample_batches WHERE batch_id=?",
                    (batch_id,),
                ).fetchone()["n"]
                out["active_sample_refs"] = conn.execute(
                    "SELECT COUNT(*) AS n FROM sample_batches sb"
                    " JOIN batches b ON b.batch_id=sb.batch_id"
                    " WHERE sb.batch_id=? AND b.status='active'",
                    (batch_id,),
                ).fetchone()["n"]
                return out
            finally:
                conn.execute("COMMIT")

    def list_batches(self, cell_id: str) -> List[dict]:
        with self._conn() as conn:
            conn.execute("BEGIN")
            try:
                rows = conn.execute(
                    "SELECT batch_id, cell_id, status, inserted, duplicate,"
                    " created_at, revoked_at FROM batches WHERE cell_id=?"
                    " ORDER BY rowid ASC",
                    (cell_id,),
                ).fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.execute("COMMIT")

    def list_cells(self) -> List[dict]:
        with self._conn() as conn:
            conn.execute("BEGIN")
            try:
                rows = conn.execute(
                    "SELECT c.cell_id, c.created_at, c.updated_at,"
                    " COALESCE(v.total_cycles,0) AS total_cycles,"
                    " COALESCE(v.version,0) AS version FROM cells c"
                    " LEFT JOIN report_versions v"
                    " ON v.cell_id=c.cell_id AND v.version="
                    " (SELECT MAX(version) FROM report_versions"
                    "  WHERE cell_id=c.cell_id)"
                    " ORDER BY c.cell_id"
                ).fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.execute("COMMIT")

    # ------------------------------------------------------------------ #
    # Internal helpers (call inside an existing transaction)
    # ------------------------------------------------------------------ #
    @staticmethod
    def _touch_cell(conn: sqlite3.Connection, cell_id: str) -> None:
        conn.execute(
            "INSERT INTO cells(cell_id) VALUES(?) ON CONFLICT(cell_id)"
            " DO UPDATE SET updated_at="
            "strftime('%Y-%m-%dT%H:%M:%fZ','now')",
            (cell_id,),
        )

    @staticmethod
    def _detect_for_cell(conn: sqlite3.Connection, cell_id: str) -> List[Cycle]:
        all_rows = conn.execute(
            "SELECT ts, current_ma FROM samples WHERE cell_id=? ORDER BY ts ASC",
            (cell_id,),
        ).fetchall()
        samples = [(r["ts"], r["current_ma"] / 1_000_000.0) for r in all_rows]
        return detect_cycles(samples)

    @staticmethod
    def _append_report(
        conn: sqlite3.Connection, cell_id: str, cycles: Sequence[Cycle]
    ) -> int:
        version_row = conn.execute(
            "SELECT COALESCE(MAX(version),0) AS v FROM report_versions"
            " WHERE cell_id=?",
            (cell_id,),
        ).fetchone()
        version = version_row["v"] + 1
        conn.execute(
            "INSERT INTO report_versions(cell_id, version, total_cycles)"
            " VALUES(?,?,?)",
            (cell_id, version, len(cycles)),
        )
        conn.executemany(
            "INSERT INTO cycles(cell_id, version, cycle_no, charge_ah,"
            " discharge_ah, started_at, ended_at) VALUES(?,?,?,?,?,?,?)",
            [
                (
                    cell_id,
                    version,
                    c.cycle_no,
                    c.charge_ah,
                    c.discharge_ah,
                    c.started_at,
                    c.ended_at,
                )
                for c in cycles
            ],
        )
        return version

    @staticmethod
    def _report_snapshot(conn: sqlite3.Connection, cell_id: str) -> dict:
        ver = conn.execute(
            "SELECT version, total_cycles, created_at FROM report_versions"
            " WHERE cell_id=? ORDER BY version DESC LIMIT 1",
            (cell_id,),
        ).fetchone()
        if ver is None:
            return {
                "cell_id": cell_id,
                "version": 0,
                "total_cycles": 0,
                "created_at": None,
                "cycles": [],
            }
        cycle_rows = conn.execute(
            "SELECT cycle_no, charge_ah, discharge_ah, started_at, ended_at"
            " FROM cycles WHERE cell_id=? AND version=? ORDER BY cycle_no ASC",
            (cell_id, ver["version"]),
        ).fetchall()
        return {
            "cell_id": cell_id,
            "version": ver["version"],
            "total_cycles": ver["total_cycles"],
            "created_at": ver["created_at"],
            "cycles": [dict(r) for r in cycle_rows],
        }


def _to_storage(current_a: float) -> int:
    """Store amperes as integer microamps.

    CSV currents like 1.234 A are stored as integer microamps so equality
    checks for idempotency are exact (no float text round-trip surprises).
    Precision below 1 µA is rejected by rounding consistency on read.
    """
    return round(float(current_a) * 1_000_000)
