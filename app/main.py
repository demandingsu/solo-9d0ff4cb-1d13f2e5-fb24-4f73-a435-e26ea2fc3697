"""FastAPI application: battery cell sample upload and cycle reports."""

from __future__ import annotations

import os

from fastapi import FastAPI, File, HTTPException, UploadFile
from pydantic import BaseModel

from .csv_import import CSVParseError, parse_samples_csv
from .db import ConflictError, Database, ImportError_, UnknownBatchError

DB_PATH = os.environ.get("BATTERY_DB_PATH", "/data/battery.db")

app = FastAPI(
    title="Battery Cell Cycle Capacity API",
    version="1.1.0",
    description=(
        "Upload UTC-second current samples per cell. Zero current marks "
        "segment boundaries; only zero→positive→zero→negative→zero sequences "
        "count as cycles. Gaps over 60 s truncate a sequence. Ah is the "
        "left-value current integral over each interval. Every accepted "
        "upload returns a batch id that can later be revoked."
    ),
)
db = Database(DB_PATH)


class CycleOut(BaseModel):
    cycle_no: int
    charge_ah: float
    discharge_ah: float
    started_at: int
    ended_at: int


class ReportOut(BaseModel):
    cell_id: str
    version: int
    total_cycles: int
    created_at: str | None
    cycles: list[CycleOut]


class UploadOut(BaseModel):
    cell_id: str
    batch_id: str
    inserted: int
    duplicate: int
    version: int
    total_cycles: int


class BatchOut(BaseModel):
    batch_id: str
    cell_id: str
    status: str
    inserted: int
    duplicate: int
    created_at: str
    revoked_at: str | None
    sample_refs: int
    active_sample_refs: int


class RevokeOut(BaseModel):
    batch_id: str
    cell_id: str
    status: str
    already_revoked: bool
    deleted_samples: int
    retained_samples: int
    version: int
    total_cycles: int
    created_at: str | None
    cycles: list[CycleOut]


@app.post(
    "/cells/{cell_id}/samples",
    response_model=UploadOut,
    tags=["upload"],
    summary="Upload a CSV batch of (UTC seconds, current A) samples",
)
async def upload_samples(cell_id: str, file: UploadFile = File(...)):
    content = await file.read()
    try:
        rows = parse_samples_csv(content)
        result = db.import_batch(cell_id, rows)
    except CSVParseError as exc:
        raise HTTPException(status_code=400, detail=f"CSV error: {exc}") from exc
    except ImportError_ as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail=(
                f"batch rejected, no data changed: same timestamp {exc.ts} "
                f"already stored with {exc.existing} A, incoming {exc.incoming} A"
            ),
        ) from exc
    return result


@app.post(
    "/batches/{batch_id}/revoke",
    response_model=RevokeOut,
    tags=["upload"],
    summary="Revoke one uploaded batch by its id",
)
def revoke_batch(batch_id: str):
    """Remove the batch's samples that have no other active basis.

    Shared samples (also submitted by another non-revoked batch) and legacy
    samples that predate batches are kept. Only when the last basis is
    revoked is a sample deleted and the report recomputed into a new
    version. Repeating a revocation is a no-op: the report is unchanged.
    """
    try:
        return db.revoke_batch(batch_id)
    except UnknownBatchError as exc:
        raise HTTPException(status_code=404, detail=f"unknown batch {batch_id}") from exc


@app.delete(
    "/batches/{batch_id}",
    response_model=RevokeOut,
    tags=["upload"],
    summary="Revoke one uploaded batch (DELETE alias)",
)
def revoke_batch_delete(batch_id: str):
    return revoke_batch(batch_id)


@app.get(
    "/batches/{batch_id}",
    response_model=BatchOut,
    tags=["query"],
    summary="Batch metadata and how many sample references it holds",
)
def get_batch(batch_id: str):
    batch = db.get_batch(batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"unknown batch {batch_id}")
    return batch


@app.get(
    "/cells/{cell_id}/batches",
    tags=["query"],
    summary="All batches uploaded for one cell (newest last)",
)
def list_batches(cell_id: str):
    return db.list_batches(cell_id)


@app.get("/cells", tags=["query"], summary="List all cells with latest report info")
def list_cells():
    return db.list_cells()


@app.get(
    "/cells/{cell_id}/report",
    response_model=ReportOut,
    tags=["query"],
    summary="Latest committed cycle report for one cell",
)
def get_report(cell_id: str):
    report = db.get_report(cell_id)
    if report is None:
        raise HTTPException(status_code=404, detail=f"unknown cell {cell_id}")
    return report


@app.get("/health", tags=["ops"], summary="Liveness probe")
def health():
    return {"status": "ok"}
