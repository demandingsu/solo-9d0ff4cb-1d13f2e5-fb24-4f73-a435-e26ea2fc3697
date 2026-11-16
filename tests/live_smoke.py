"""Live-server smoke test.

Runs against an already-started uvicorn (default http://127.0.0.1:8000).
Covers: late-point rewrite/renumbering, gap truncation, idempotency and
conflict rejection, batch id return, batch revocation (shared samples,
repeat revocation, unknown batch) and concurrent uploads/revocations/queries
with snapshot consistency.
"""

import concurrent.futures
import sys
import time

import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"


def upload(client, cell, rows, header="timestamp,current"):
    text = header + "\n" + "".join(f"{ts},{cur}\n" for ts, cur in rows)
    return client.post(
        f"{BASE}/cells/{cell}/samples",
        files={"file": ("batch.csv", text, "text/csv")},
    )


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)
    print(f"  PASS: {msg}")


def main():
    with httpx.Client(timeout=30) as c:
        r = c.get(f"{BASE}/health")
        check(r.status_code == 200, "health endpoint")

        # --- 1. late point rewrites/renumbers cycles --------------------
        cell = "LIVE-LATE"
        r = upload(c, cell, [(0, 0), (10, 1), (20, 0), (30, -1), (40, 0), (50, 1)])
        check(r.status_code == 200, "first batch accepted")
        check(r.json()["total_cycles"] == 1, "one closed cycle, tail ignored")
        v1 = r.json()["version"]

        r = upload(c, cell, [(60, 0), (70, -1), (80, 0)])
        check(r.status_code == 200, "late batch accepted")
        check(r.json()["version"] == v1 + 1, "report version bumped")
        rep = c.get(f"{BASE}/cells/{cell}/report").json()
        check(rep["total_cycles"] == 2, "late point closed second cycle")
        check([x["cycle_no"] for x in rep["cycles"]] == [1, 2], "cycles renumbered 1..N")
        check(
            abs(rep["cycles"][0]["charge_ah"] - 1 * 10 / 3600) < 1e-12,
            "charge Ah from left-value integral",
        )

        # --- 2. gap > 60 s truncates ------------------------------------
        cell = "LIVE-GAP"
        r = upload(c, cell, [(0, 0), (10, 2), (71, 0), (80, -3), (90, 0)])
        rep = c.get(f"{BASE}/cells/{cell}/report").json()
        check(rep["total_cycles"] == 0, "61 s gap truncates the sequence")

        r = upload(c, cell, [(0, 0), (60, 2), (120, 0), (130, -1), (140, 0)])
        rep = c.get(f"{BASE}/cells/{cell}/report").json()
        check(rep["total_cycles"] == 1, "exactly-60 s intervals are kept")

        # --- 3. idempotency + conflict whole-batch rejection ------------
        cell = "LIVE-IDEM"
        rows = [(0, 0), (10, 1.5), (20, 0), (30, -2.5), (40, 0)]
        first = upload(c, cell, rows).json()
        check(bool(first.get("batch_id")), "upload returns a batch id")
        r = upload(c, cell, rows)
        check(r.json()["inserted"] == 0 and r.json()["duplicate"] == 5,
              "identical re-upload is idempotent")
        dup_batch = r.json()["batch_id"]
        check(dup_batch != first["batch_id"],
              "all-duplicate upload still gets its own batch id")
        before = c.get(f"{BASE}/cells/{cell}/report").json()
        r = upload(c, cell, [(10, 9.9), (99, 1)])
        check(r.status_code == 409, "different value at same ts -> 409")
        after = c.get(f"{BASE}/cells/{cell}/report").json()
        check(after["version"] == before["version"], "rejected batch keeps old version")
        check(after["total_cycles"] == before["total_cycles"], "old report unchanged")

        # --- 3b. batch revocation semantics -----------------------------
        cell = "LIVE-REVOKE"
        shared = [(0, 0), (10, 1)]
        a = upload(c, cell, shared + [(20, 0), (30, -1), (40, 0)]).json()
        b = upload(c, cell, shared + [(20, 0), (30, -2), (40, 0)])
        check(b.status_code == 409, "conflicting duplicate is rejected")
        b = upload(c, cell, shared).json()
        check(b["duplicate"] == 2, "second batch shares two sample points")

        rv = c.post(f"{BASE}/batches/{a['batch_id']}/revoke")
        check(rv.status_code == 200, "revoke accepts a known batch id")
        check(rv.json()["retained_samples"] == 2,
              "shared sample survives while another batch backs it")
        check(rv.json()["deleted_samples"] == 3, "unique samples are deleted")
        rep = c.get(f"{BASE}/cells/{cell}/report").json()
        check(rep["total_cycles"] == 0, "report recomputed after partial revoke")

        again = c.post(f"{BASE}/batches/{a['batch_id']}/revoke").json()
        check(again["already_revoked"] and again["version"] == rep["version"],
              "repeat revoke does not change the report")
        check(c.post(f"{BASE}/batches/no-such-batch/revoke").status_code == 404,
              "unknown batch -> 404")

        rv = c.post(f"{BASE}/batches/{b['batch_id']}/revoke").json()
        check(rv["deleted_samples"] == 2, "last basis revoked deletes shared points")
        rep = c.get(f"{BASE}/cells/{cell}/report").json()
        check(rep["total_cycles"] == 0 and rep["cycles"] == [],
              "empty sample set yields an empty report")

        # --- 4. concurrency: 8 writers, 8 readers, 4 revokers -----------
        cell = "LIVE-CONC"

        def writer(n):
            base = n * 1000
            with httpx.Client(timeout=30) as wc:
                return upload(
                    wc, cell,
                    [(base, 0), (base + 10, 1), (base + 20, 0),
                     (base + 30, -1), (base + 40, 0)],
                ).status_code

        def reader(_):
            bad = []
            with httpx.Client(timeout=30) as rc:
                for _ in range(40):
                    rr = rc.get(f"{BASE}/cells/{cell}/report")
                    if rr.status_code == 404:
                        continue
                    rep = rr.json()
                    nos = [x["cycle_no"] for x in rep["cycles"]]
                    if rep["total_cycles"] != len(rep["cycles"]) or nos != list(
                        range(1, rep["total_cycles"] + 1)
                    ):
                        bad.append(rep)
            return bad

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
            f_w = [pool.submit(writer, n) for n in range(8)]
            f_r = [pool.submit(reader, n) for n in range(8)]
            codes = [f.result() for f in f_w]
            inconsistencies = [x for f in f_r for x in f.result()]
        check(codes == [200] * 8, f"all 8 concurrent writers committed (got {codes})")
        check(not inconsistencies, "every query saw one internally consistent state")
        final = c.get(f"{BASE}/cells/{cell}/report").json()
        check(final["total_cycles"] == 8, "final report contains all 8 cycles")

        batches = c.get(f"{BASE}/cells/{cell}/batches").json()
        check(len(batches) == 8, "every upload is listed as a batch")

        # revoke four batches concurrently with readers; every observed
        # report must still be internally consistent
        def revoker(batch_id):
            with httpx.Client(timeout=30) as rc:
                return rc.post(f"{BASE}/batches/{batch_id}/revoke").status_code

        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            f_rv = [pool.submit(revoker, b["batch_id"]) for b in batches[:4]]
            f_r2 = [pool.submit(reader, n) for n in range(8)]
            rv_codes = [f.result() for f in f_rv]
            inconsistencies = [x for f in f_r2 for x in f.result()]
        check(rv_codes == [200] * 4, f"4 concurrent revokes succeeded (got {rv_codes})")
        check(not inconsistencies, "queries consistent during concurrent revokes")
        final = c.get(f"{BASE}/cells/{cell}/report").json()
        check(final["total_cycles"] == 4, "four revoked cycles removed")
        check([x["cycle_no"] for x in final["cycles"]] == [1, 2, 3, 4],
              "cycle numbering recomputed 1..N")

        cells = c.get(f"{BASE}/cells").json()
        check(any(x["cell_id"] == cell for x in cells), "cell list endpoint")

    print("\nALL LIVE CHECKS PASSED")


if __name__ == "__main__":
    for attempt in range(20):
        try:
            httpx.get(f"{BASE}/health", timeout=2)
            break
        except httpx.HTTPError:
            time.sleep(0.5)
    main()
