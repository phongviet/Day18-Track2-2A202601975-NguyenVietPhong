"""Smoke test for the lightweight path. Run via `make smoke`.

Covers every capability the eight notebooks depend on, and does so **fully
offline** — no DuckDB extension download, no model, no API key, no Docker.
If this passes, the lab will run on a plane.
"""
from __future__ import annotations

import shutil
import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import numpy as np
import polars as pl
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from lakehouse import catalog, namespace, path, reset, reset_catalog

PASS, FAIL = "  ✓", "  ✗"


def step(label: str) -> None:
    print(f"{PASS} {label}")


def check_delta() -> None:
    """NB1–NB4, NB6, NB7: write, read, time travel, history."""
    p = path("scratch", "_smoke_delta")
    reset(p)
    df = pl.DataFrame({"n": list(range(10))})
    write_deltalake(p, df.to_arrow(), mode="overwrite")
    assert DeltaTable(p).to_pyarrow_table().num_rows == 10
    step("delta write + read")

    write_deltalake(p, df.to_arrow(), mode="append")
    assert DeltaTable(p, version=0).to_pyarrow_table().num_rows == 10
    assert len(DeltaTable(p).history()) >= 2
    step("delta time travel + history")

    dt = DeltaTable(p)
    assert hasattr(dt, "file_uris"), "delta-rs 1.x expected (dt.files() was removed)"
    dt.optimize.compact()
    assert dt.vacuum(retention_hours=0, dry_run=True, enforce_retention_duration=False) is not None
    step("delta maintenance (compact / vacuum) — NB6")


def check_cdf() -> None:
    """NB7: change data feed carries deletes."""
    p = path("scratch", "_smoke_cdf")
    reset(p)
    write_deltalake(p, pa.table({"id": list(range(20))}), mode="overwrite",
                    configuration={"delta.enableChangeDataFeed": "true"})
    DeltaTable(p).delete("id < 5")
    cdf = DeltaTable(p).load_cdf(starting_version=1).read_all()
    kinds = set(cdf.column("_change_type").to_pylist())
    assert "delete" in kinds, f"expected delete events, got {kinds}"
    step("delta change data feed — NB7")


def check_iceberg() -> None:
    """NB5, NB6, NB8: catalog, hidden partitioning, scan planning."""
    import datetime as dtm

    from pyiceberg.transforms import DayTransform

    reset_catalog("smoke")
    cat = catalog("smoke")
    ns = namespace(cat, "smoke")
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False),
                        pa.field("ts", pa.timestamp("us"), nullable=False)])
    tbl = cat.create_table(f"{ns}.t", schema=schema)
    with tbl.update_spec() as spec:
        spec.add_field("ts", DayTransform(), "ts_day")
    tbl = cat.load_table(f"{ns}.t")

    for d in range(1, 6):
        tbl.append(pa.table({"id": list(range(d * 10, d * 10 + 10)),
                             "ts": [dtm.datetime(2026, 8, d)] * 10}, schema=schema))
    tbl = cat.load_table(f"{ns}.t")
    step("iceberg catalog + append")

    all_files = len(list(tbl.scan().plan_files()))
    one_day = len(list(tbl.scan(
        row_filter="ts >= '2026-08-03T00:00:00' and ts < '2026-08-04T00:00:00'").plan_files()))
    assert all_files > one_day, f"hidden partitioning did not prune ({all_files} vs {one_day})"
    step(f"iceberg scan planning prunes {all_files} → {one_day} files")

    assert hasattr(tbl.maintenance, "expire_snapshots")
    step("iceberg maintenance API present — NB6")
    reset_catalog("smoke")


def check_vectors() -> None:
    """NB7: vector search with DuckDB core functions — no extension download."""
    import duckdb

    rng = np.random.default_rng(0)
    dim, n = 16, 200
    emb = rng.normal(size=(n, dim)).astype("float32")
    emb /= np.linalg.norm(emb, axis=1, keepdims=True)
    tbl = pa.table({
        "id": list(range(n)),
        "emb": pa.FixedSizeListArray.from_arrays(pa.array(emb.ravel(), pa.float32()), dim),
    })
    con = duckdb.connect()
    con.register("v", tbl)
    q = emb[3].tolist()
    top = con.sql(f"""SELECT id FROM v
                      ORDER BY array_cosine_similarity(emb::FLOAT[{dim}], {q}::FLOAT[{dim}]) DESC
                      LIMIT 1""").fetchone()[0]
    assert top == 3, f"nearest neighbour of vector 3 should be 3, got {top}"
    step("duckdb vector search (core, offline)")


def check_duckdb_delta_bridge() -> None:
    """DuckDB reads Delta via Arrow — deliberately NOT via delta_scan().

    `delta_scan()` autoloads a DuckDB extension from the internet. That turns a
    firewalled classroom into a support ticket, so the notebooks hand DuckDB an
    Arrow table instead. Same SQL, zero network.
    """
    import duckdb

    p = path("scratch", "_smoke_delta")
    con = duckdb.connect()
    con.register("t", DeltaTable(p).to_pyarrow_table())
    assert con.sql("SELECT count(*) FROM t").fetchone()[0] == 20
    step("duckdb ↔ delta via arrow (no extension download)")


def main() -> int:
    print("Lakehouse smoke test — lightweight path (offline)\n")
    try:
        check_delta()
        check_cdf()
        check_iceberg()
        check_vectors()
        check_duckdb_delta_bridge()
    except Exception as e:  # noqa: BLE001
        print(f"\n{FAIL} Smoke test FAILED: {type(e).__name__}: {e}\n")
        traceback.print_exc()
        return 1
    finally:
        shutil.rmtree(path("scratch", "_smoke_delta"), ignore_errors=True)
        shutil.rmtree(path("scratch", "_smoke_cdf"), ignore_errors=True)

    print("\nAll checks passed — the lab is ready. Run `make data && make lab`.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
