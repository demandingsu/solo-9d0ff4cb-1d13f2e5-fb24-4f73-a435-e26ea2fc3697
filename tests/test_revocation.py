"""Batch identity + revocation tests (shared samples, legacy DB, interleaving)."""

import os
import sqlite3
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_TMP = tempfile.mkdtemp(prefix="battery-revoke-")
_DB = os.path.join(_TMP, "test.db")
os.environ["BATTERY_DB_PATH"] = _DB

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app, db as app_db  # noqa: E402

# app.main is a module-level singleton; when the whole suite runs another
# test module may have set BATTERY_DB_PATH first, so always ask the app
# object for the database it actually opened.
DB_FILE = app_db.db_path


def csv_text(rows, header="timestamp,current\n"):
    return header + "".join(f"{ts},{cur}\n" for ts, cur in rows)


def upload(client, cell_id, rows):
    return client.post(
        f"/cells/{cell_id}/samples",
        files={"file": ("samples.csv", csv_text(rows), "text/csv")},
    )


CYCLE_1 = [(0, 0.0), (10, 1.0), (20, 0.0), (30, -1.0), (40, 0.0)]
CYCLE_2 = [(50, 0.0), (60, 2.0), (70, 0.0), (80, -2.0), (90, 0.0)]


class BatchIdTests(unittest.TestCase):
    def setUp(self):
        self.c = TestClient(app)

    def test_upload_returns_batch_id(self):
        r = upload(self.c, "R-BATCHID", CYCLE_1)
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertIsInstance(body.get("batch_id"), str)
        self.assertTrue(body["batch_id"])
        self.assertEqual(body["version"], 1)

    def test_all_duplicate_upload_still_returns_new_batch_id(self):
        cell = "R-ALLDUP"
        first = upload(self.c, cell, CYCLE_1).json()
        again = upload(self.c, cell, CYCLE_1)
        self.assertEqual(again.status_code, 200, again.text)
        body = again.json()
        self.assertEqual(body["inserted"], 0)
        self.assertEqual(body["duplicate"], len(CYCLE_1))
        self.assertNotEqual(body["batch_id"], first["batch_id"])
        self.assertEqual(body["version"], first["version"] + 1)

        batch = self.c.get(f"/batches/{body['batch_id']}").json()
        self.assertEqual(batch["status"], "active")
        self.assertEqual(batch["sample_refs"], len(CYCLE_1))
        self.assertEqual(batch["active_sample_refs"], len(CYCLE_1))

    def test_list_batches_for_cell(self):
        cell = "R-LIST"
        b1 = upload(self.c, cell, CYCLE_1).json()["batch_id"]
        b2 = upload(self.c, cell, CYCLE_2).json()["batch_id"]
        listed = self.c.get(f"/cells/{cell}/batches").json()
        self.assertEqual([x["batch_id"] for x in listed], [b1, b2])


class RevocationTests(unittest.TestCase):
    def setUp(self):
        self.c = TestClient(app)

    def test_revoke_removes_only_that_batch_and_recomputes(self):
        cell = "R-SOLO"
        b1 = upload(self.c, cell, CYCLE_1).json()
        r2 = upload(self.c, cell, CYCLE_2).json()
        rep = self.c.get(f"/cells/{cell}/report").json()
        self.assertEqual(rep["total_cycles"], 2)

        rev = self.c.post(f"/batches/{r2['batch_id']}/revoke")
        self.assertEqual(rev.status_code, 200, rev.text)
        body = rev.json()
        self.assertEqual(body["deleted_samples"], len(CYCLE_2))
        self.assertEqual(body["retained_samples"], 0)
        self.assertEqual(body["total_cycles"], 1)
        self.assertEqual([x["cycle_no"] for x in body["cycles"]], [1])

        # same state via the query endpoint
        rep = self.c.get(f"/cells/{cell}/report").json()
        self.assertEqual(rep["version"], body["version"])
        self.assertEqual(rep["total_cycles"], 1)
        self.assertEqual(rep["cycles"][0]["started_at"], 0)
        self.assertEqual(rep["cycles"][0]["ended_at"], 40)

        # now revoke the first batch: everything it inserted goes away
        rev1 = self.c.post(f"/batches/{b1['batch_id']}/revoke").json()
        self.assertEqual(rev1["deleted_samples"], len(CYCLE_1))
        self.assertEqual(rev1["total_cycles"], 0)
        self.assertEqual(rev1["cycles"], [])
        self.assertGreater(rev1["version"], rep["version"])

    def test_shared_sample_survives_until_last_basis_revoked(self):
        cell = "R-SHARED"
        shared = [(0, 0.0), (10, 1.0)]
        extra_a = [(20, 0.0), (30, -1.0), (40, 0.0)]
        extra_b = [(20, 0.0), (30, -2.0), (40, 0.0)]

        a = upload(self.c, cell, shared + extra_a).json()
        b = upload(self.c, cell, shared + extra_b)  # collides at 30 != -1 -> 409
        self.assertEqual(b.status_code, 409, b.text)

        # share only the identical prefix; give B its own disjoint tail
        b = upload(self.c, cell, shared).json()
        self.assertEqual(b["duplicate"], 2)

        # revoke A: shared points still backed by B, A-only tail removed
        rev_a = self.c.post(f"/batches/{a['batch_id']}/revoke").json()
        self.assertEqual(rev_a["retained_samples"], 2)
        self.assertEqual(rev_a["deleted_samples"], len(extra_a))
        self.assertEqual(rev_a["total_cycles"], 0)

        points = self._points(cell)
        self.assertIn(0, points)
        self.assertIn(10, points)
        self.assertNotIn(20, points)

        # revoke B -> shared points lose their last basis and disappear
        rev_b = self.c.post(f"/batches/{b['batch_id']}/revoke").json()
        self.assertEqual(rev_b["deleted_samples"], 2)
        self.assertEqual(rev_b["total_cycles"], 0)
        self.assertEqual(self._points(cell), [])

    def test_repeated_revoke_is_noop_and_does_not_change_report(self):
        cell = "R-REPEAT"
        b = upload(self.c, cell, CYCLE_1).json()
        upload(self.c, cell, CYCLE_2)

        first = self.c.post(f"/batches/{b['batch_id']}/revoke").json()
        self.assertFalse(first["already_revoked"])
        report_after_first = self.c.get(f"/cells/{cell}/report").json()

        second = self.c.post(f"/batches/{b['batch_id']}/revoke")
        self.assertEqual(second.status_code, 200, second.text)
        sb = second.json()
        self.assertTrue(sb["already_revoked"])
        self.assertEqual(sb["deleted_samples"], 0)
        # report version and details are unchanged
        self.assertEqual(sb["version"], report_after_first["version"])
        self.assertEqual(sb["total_cycles"], report_after_first["total_cycles"])
        self.assertEqual(sb["cycles"], report_after_first["cycles"])
        self.assertEqual(
            self.c.get(f"/cells/{cell}/report").json(), report_after_first
        )

        # DELETE alias behaves the same
        alias = self.c.delete(f"/batches/{b['batch_id']}")
        self.assertEqual(alias.status_code, 200)
        self.assertTrue(alias.json()["already_revoked"])

    def test_unknown_batch_404_and_state_unchanged(self):
        cell = "R-UNKNOWN"
        upload(self.c, cell, CYCLE_1)
        before = self.c.get(f"/cells/{cell}/report").json()

        r = self.c.post("/batches/does-not-exist/revoke")
        self.assertEqual(r.status_code, 404, r.text)
        self.assertEqual(self.c.get("/batches/does-not-exist").status_code, 404)
        self.assertEqual(self.c.get(f"/cells/{cell}/report").json(), before)

        # revoking a batch id under a different cell path is simply unknown
        b = upload(self.c, "R-OTHER-CELL", CYCLE_2).json()
        self.assertEqual(
            self.c.get(f"/batches/{b['batch_id']}").status_code, 200
        )
        self.assertEqual(
            self.c.post(f"/batches/{b['batch_id']}/revoke").json()["cell_id"],
            "R-OTHER-CELL",
        )

    def test_failed_revoke_keeps_business_state(self):
        cell = "R-FAIL"
        b = upload(self.c, cell, CYCLE_1).json()
        before = self.c.get(f"/cells/{cell}/report").json()
        failed = self.c.post("/batches/unknown-id/revoke")
        self.assertEqual(failed.status_code, 404)
        # batch is still active and its samples are untouched
        self.assertEqual(self.c.get(f"/batches/{b['batch_id']}").json()["status"],
                         "active")
        self.assertEqual(len(self._points(cell)), len(CYCLE_1))
        self.assertEqual(self.c.get(f"/cells/{cell}/report").json(), before)

    def test_revoke_crash_mid_transaction_rolls_back_everything(self):
        """If recomputation raises, samples/batch/report stay as committed."""
        from unittest import mock

        import app.db as db_mod
        from app.db import Database

        cell = "R-CRASH"
        b = upload(self.c, cell, CYCLE_1).json()
        before = self.c.get(f"/cells/{cell}/report").json()

        local = Database(DB_FILE)
        real_detect = db_mod.detect_cycles

        def boom(samples):
            # Empty sample set means the delete already happened in-txn.
            if not samples:
                raise RuntimeError("injected recompute failure")
            return real_detect(samples)

        with mock.patch.object(db_mod, "detect_cycles", side_effect=boom):
            with self.assertRaises(RuntimeError):
                local.revoke_batch(b["batch_id"])

        # rollback restored the batch status, samples and report
        self.assertEqual(self.c.get(f"/batches/{b['batch_id']}").json()["status"],
                         "active")
        self.assertEqual(len(self._points(cell)), len(CYCLE_1))
        self.assertEqual(self.c.get(f"/cells/{cell}/report").json(), before)

        # and the batch can be revoked for real afterwards
        rev = self.c.post(f"/batches/{b['batch_id']}/revoke")
        self.assertEqual(rev.status_code, 200, rev.text)
        self.assertFalse(rev.json()["already_revoked"])
        self.assertEqual(rev.json()["deleted_samples"], len(CYCLE_1))

    def _points(self, cell):
        conn = sqlite3.connect(DB_FILE)
        try:
            return [
                r[0]
                for r in conn.execute(
                    "SELECT ts FROM samples WHERE cell_id=? ORDER BY ts", (cell,)
                )
            ]
        finally:
            conn.close()


class LegacyDatabaseTests(unittest.TestCase):
    def test_pre_batch_samples_are_kept_on_revoke(self):
        """A database created by the old code (no batch tables) migrates in place."""
        path = os.path.join(_TMP, "legacy.db")
        raw = sqlite3.connect(path)
        raw.executescript(
            """
            CREATE TABLE cells (
                cell_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL DEFAULT
                    (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at TEXT NOT NULL DEFAULT
                    (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
            CREATE TABLE samples (
                cell_id TEXT NOT NULL,
                ts INTEGER NOT NULL,
                current_ma INTEGER NOT NULL,
                PRIMARY KEY (cell_id, ts)
            );
            CREATE TABLE report_versions (
                cell_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                total_cycles INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT
                    (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                PRIMARY KEY (cell_id, version)
            );
            CREATE TABLE cycles (
                cell_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                cycle_no INTEGER NOT NULL,
                charge_ah REAL NOT NULL,
                discharge_ah REAL NOT NULL,
                started_at INTEGER NOT NULL,
                ended_at INTEGER NOT NULL,
                PRIMARY KEY (cell_id, version, cycle_no)
            );
            CREATE TABLE upload_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cell_id TEXT NOT NULL,
                status TEXT NOT NULL,
                detail TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT
                    (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
            INSERT INTO cells(cell_id) VALUES ('LEGACY');
            INSERT INTO report_versions(cell_id, version, total_cycles)
                VALUES ('LEGACY', 1, 1);
            INSERT INTO cycles VALUES ('LEGACY', 1, 1, 0.005, 0.005, 0, 40);
            INSERT INTO samples VALUES ('LEGACY', 0, 0);
            INSERT INTO samples VALUES ('LEGACY', 10, 1000000);
            INSERT INTO samples VALUES ('LEGACY', 20, 0);
            INSERT INTO samples VALUES ('LEGACY', 30, -1000000);
            INSERT INTO samples VALUES ('LEGACY', 40, 0);
            """
        )
        raw.commit()
        raw.close()

        # The app singleton's database is untouched by this isolated legacy
        # instance; only the migrated legacy file is exercised here.
        from app.db import Database

        legacy_db = Database(path)
        # old samples readable through the same snapshot logic
        rep = legacy_db.get_report("LEGACY")
        self.assertEqual(rep["version"], 1)
        self.assertEqual(rep["total_cycles"], 1)

        # add a batch touching the same timestamp 10 -> duplicate, origin stays NULL
        res = legacy_db.import_batch("LEGACY", [(10, 1.0), (50, 2.0), (60, 0.0)])
        self.assertEqual(res["duplicate"], 1)
        self.assertEqual(res["inserted"], 2)

        rev = legacy_db.revoke_batch(res["batch_id"])
        # the shared legacy point at ts=10 is retained (no origin batch)
        self.assertEqual(rev["retained_samples"], 1)
        self.assertEqual(rev["deleted_samples"], 2)
        conn = sqlite3.connect(path)
        try:
            ts = {
                r[0]
                for r in conn.execute(
                    "SELECT ts FROM samples WHERE cell_id='LEGACY'"
                )
            }
            cols = {r[1] for r in conn.execute("PRAGMA table_info(samples)")}
        finally:
            conn.close()
        self.assertTrue({0, 10, 20, 30, 40}.issubset(ts))
        self.assertNotIn(50, ts)
        self.assertIn("origin_batch_id", cols)

        # revoking again is idempotent and keeps the legacy points
        again = legacy_db.revoke_batch(res["batch_id"])
        self.assertTrue(again["already_revoked"])
        self.assertEqual(legacy_db.get_report("LEGACY")["total_cycles"], 1)


class InterleavingConsistencyTests(unittest.TestCase):
    def test_interleaved_upload_revoke_query_are_consistent(self):
        """Every query sees batch basis, samples and report from one commit."""
        cell = "R-CONC"
        c = TestClient(app)
        batch_ids = []
        errors = []
        lock = threading.Lock()

        def writer(n):
            try:
                client = TestClient(app)
                base = n * 1000
                rows = [
                    (base + 0, 0.0),
                    (base + 10, 1.0),
                    (base + 20, 0.0),
                    (base + 30, -1.0),
                    (base + 40, 0.0),
                ]
                r = upload(client, cell, rows)
                if r.status_code != 200:
                    with lock:
                        errors.append(f"writer {n}: {r.status_code} {r.text}")
                else:
                    with lock:
                        batch_ids.append(r.json()["batch_id"])
            except Exception as exc:  # pragma: no cover
                with lock:
                    errors.append(repr(exc))

        def revoker(n):
            try:
                client = TestClient(app)
                for _ in range(20):
                    with lock:
                        candidates = list(batch_ids)
                    for bid in candidates:
                        rr = client.post(f"/batches/{bid}/revoke")
                        if rr.status_code not in (200, 404):
                            with lock:
                                errors.append(
                                    f"revoke {bid}: {rr.status_code} {rr.text}"
                                )
            except Exception as exc:  # pragma: no cover
                with lock:
                    errors.append(repr(exc))

        def reader(n):
            try:
                client = TestClient(app)
                for _ in range(40):
                    rr = client.get(f"/cells/{cell}/report")
                    if rr.status_code == 404:
                        continue
                    rep = rr.json()
                    nos = [x["cycle_no"] for x in rep["cycles"]]
                    if rep["total_cycles"] != len(rep["cycles"]) or nos != list(
                        range(1, rep["total_cycles"] + 1)
                    ):
                        with lock:
                            errors.append(f"inconsistent report: {rep}")
            except Exception as exc:  # pragma: no cover
                with lock:
                    errors.append(repr(exc))

        threads = [threading.Thread(target=writer, args=(n,)) for n in range(4)]
        threads += [threading.Thread(target=revoker, args=(n,)) for n in range(2)]
        threads += [threading.Thread(target=reader, args=(n,)) for n in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        final = TestClient(app).get(f"/cells/{cell}/report").json()
        self.assertEqual(final["total_cycles"], len(final["cycles"]))
        self.assertEqual(
            [x["cycle_no"] for x in final["cycles"]],
            list(range(1, final["total_cycles"] + 1)),
        )

        # Revoking every batch for the cell must remove every sample it
        # created; no orphan point may survive.
        listed = TestClient(app).get(f"/cells/{cell}/batches").json()
        for b in listed:
            self.assertEqual(
                TestClient(app).post(f"/batches/{b['batch_id']}/revoke").status_code,
                200,
            )
        conn = sqlite3.connect(DB_FILE)
        try:
            left = conn.execute(
                "SELECT COUNT(*) FROM samples WHERE cell_id=?", (cell,)
            ).fetchone()[0]
            refs = conn.execute(
                "SELECT COUNT(*) FROM sample_batches sb JOIN batches b"
                " ON b.batch_id=sb.batch_id"
                " WHERE sb.cell_id=? AND b.status='active'", (cell,)
            ).fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(left, 0)
        self.assertEqual(refs, 0)
        self.assertEqual(
            TestClient(app).get(f"/cells/{cell}/report").json()["total_cycles"], 0
        )

    def test_interleaved_db_consistency_of_all_versions(self):
        """report_versions total must always match its cycle detail rows."""
        conn = sqlite3.connect(DB_FILE)
        try:
            rows = conn.execute(
                "SELECT v.cell_id, v.version, v.total_cycles,"
                " COUNT(c.cycle_no) FROM report_versions v"
                " LEFT JOIN cycles c ON c.cell_id=v.cell_id"
                " AND c.version=v.version GROUP BY v.cell_id, v.version"
            ).fetchall()
        finally:
            conn.close()
        for cell_id, version, total, detail in rows:
            self.assertEqual(
                total, detail, f"{cell_id} v{version} total != detail count"
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
