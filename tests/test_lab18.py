"""Fast invariants for the Day 18 lakehouse lab.

These are the checks that must hold for the notebooks to be *runnable* — the
per-notebook pass criteria live in the notebooks themselves and are exercised
by `make run-all`. Keep this suite under ~30s so it is cheap to run often.
"""
from __future__ import annotations

import datetime as dtm
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from deltalake import DeltaTable, write_deltalake  # noqa: E402

import generate_ai_data as gen  # noqa: E402
import lakehouse as lh  # noqa: E402


# ── environment ────────────────────────────────────────────────────────────

def test_python_version_supported():
    assert (3, 10) <= sys.version_info[:2] < (3, 15), (
        "lab targets Python 3.10–3.14; update requirements.txt if this changes"
    )


def test_delta_rs_is_v1():
    """NB2/NB6 use file_uris(), load_cdf() and repair() — all 1.x-only."""
    import deltalake

    assert int(deltalake.__version__.split(".")[0]) >= 1, deltalake.__version__
    for attr in ("file_uris", "load_cdf", "repair", "create_checkpoint", "compact_logs"):
        assert hasattr(DeltaTable, attr), f"delta-rs missing {attr}"


def test_duckdb_has_core_vector_functions():
    """array_cosine_similarity must be CORE — an extension download would
    break the lab on a firewalled network."""
    import duckdb

    con = duckdb.connect()
    got = con.sql("SELECT array_cosine_similarity([1.0,0.0]::FLOAT[2], "
                  "[1.0,0.0]::FLOAT[2])").fetchone()[0]
    assert got == pytest.approx(1.0)


# ── shared helpers ─────────────────────────────────────────────────────────

def test_human_readable_sizes():
    assert lh.human(512) == "512 B"
    assert lh.human(1536).endswith("KB")
    assert lh.human(5 * 1024 ** 3).endswith("GB")


def test_du_missing_path_is_zero():
    assert lh.du("/nonexistent/path/xyz") == 0


def test_to_arrow_normalises_duckdb_result():
    """DuckDB ≥1.3 returns a RecordBatchReader; notebooks need a Table."""
    import duckdb

    out = lh.to_arrow(duckdb.sql("SELECT 1 AS a"))
    assert isinstance(out, pa.Table) and out.num_rows == 1


# ── corpus generator (NB7 / NB8) ───────────────────────────────────────────

def test_corpus_is_deterministic():
    a, b = gen.make_corpus(200), gen.make_corpus(200)
    assert a.column("doc_id").to_pylist() == b.column("doc_id").to_pylist()
    assert np.allclose(np.array(a.column("emb").to_pylist()),
                       np.array(b.column("emb").to_pylist()))


def test_embeddings_are_topic_clustered():
    """A random corpus makes every retrieval metric in NB7 meaningless."""
    t = gen.make_corpus(600)
    emb = np.array(t.column("emb").to_pylist(), dtype="float32")
    topics = np.array(t.column("topic").to_pylist())
    sims = emb @ emb.T
    np.fill_diagonal(sims, -9)
    nn_same_topic = (topics[sims.argmax(1)] == topics).mean()
    assert nn_same_topic > 0.9, f"topic signal too weak: {nn_same_topic:.2f}"


def test_embeddings_are_unit_norm():
    emb = np.array(gen.make_corpus(100).column("emb").to_pylist(), dtype="float32")
    assert np.allclose(np.linalg.norm(emb, axis=1), 1.0, atol=1e-5)


def test_all_four_art10_buckets_are_representable():
    """NB8 partitions by provenance bucket; all four must actually occur."""
    t = gen.make_corpus(2000)
    licenses = set(t.column("license").to_pylist())
    assert {"proprietary", "commercial"} & licenses      # licensed
    assert "cc-by-4.0" in licenses                        # public domain
    assert "user-owned" in licenses                       # opt-out checked
    assert "synthetic" in licenses                        # synthetic
    assert "unknown" in licenses                          # the audit failure


def test_synthetic_rows_record_their_generator():
    """Art. 10 requires synthetic data to name what produced it."""
    t = gen.make_corpus(2000).to_pylist()
    syn = [r for r in t if r["license"] == "synthetic"]
    assert syn, "no synthetic rows generated"
    assert all(r["generator"] for r in syn)
    assert all(r["generator"] is None for r in t if r["license"] != "synthetic")


def test_trajectories_have_multi_step_sessions():
    traj = gen.make_trajectories(50).to_pylist()
    steps = {}
    for r in traj:
        steps[r["session_id"]] = steps.get(r["session_id"], 0) + 1
    assert max(steps.values()) > 1, "trajectories must be multi-step"
    assert {"ok", "error"} & {r["status"] for r in traj}


# ── format behaviours the notebooks teach ──────────────────────────────────

@pytest.fixture
def delta_table(tmp_path):
    p = str(tmp_path / "t")
    for i in range(5):
        write_deltalake(p, pa.table({"id": list(range(i * 10, i * 10 + 10)),
                                     "grp": [i] * 10}),
                        mode="append" if i else "overwrite")
    return p


def test_schema_enforcement_blocks_bad_writes(delta_table):
    with pytest.raises(Exception):
        write_deltalake(delta_table, pa.table({"id": ["not-an-int"], "grp": [0]}),
                        mode="append")


def test_schema_evolution_is_opt_in(delta_table):
    write_deltalake(delta_table, pa.table({"id": [99], "grp": [9], "tier": ["gold"]}),
                    mode="append", schema_mode="merge")
    assert "tier" in DeltaTable(delta_table).schema().to_arrow().names


def test_compaction_reduces_file_count(delta_table):
    before = len(DeltaTable(delta_table).file_uris())
    DeltaTable(delta_table).optimize.compact()
    after = len(DeltaTable(delta_table).file_uris())
    assert after < before, f"{before} → {after}"


def test_time_travel_survives_compaction(delta_table):
    DeltaTable(delta_table).optimize.compact()
    assert DeltaTable(delta_table, version=0).to_pyarrow_table().num_rows == 10


def test_add_action_stats_expose_min_max(delta_table):
    """NB6 measures file skipping from these stats rather than by stopwatch."""
    aa = pa.table(DeltaTable(delta_table).get_add_actions(flatten=True))
    assert "min.id" in aa.column_names and "max.id" in aa.column_names


def test_vacuum_does_not_see_uncommitted_orphans(tmp_path):
    """Measured behaviour NB6 teaches: delta-rs reclaims TOMBSTONED files only.

    If a future delta-rs adds the directory-listing pass, this test fails and
    the notebook's claim must be updated — that is the point of pinning it here.
    """
    import os
    import time

    import pyarrow.parquet as pq

    p = str(tmp_path / "t")
    write_deltalake(p, pa.table({"id": [1, 2, 3]}), mode="overwrite")
    orphan = Path(p) / "part-99999-orphan-c000.snappy.parquet"
    pq.write_table(pa.table({"id": [1]}), orphan)
    old = time.time() - 30 * 86400
    os.utime(orphan, (old, old))

    seen = DeltaTable(p).vacuum(retention_hours=0, dry_run=True,
                                enforce_retention_duration=False)
    assert not any("orphan" in f for f in seen), (
        "delta-rs now detects uncommitted orphans — update NB6's Job 4 section"
    )
    assert orphan.exists()


def test_cdf_emits_delete_events(tmp_path):
    p = str(tmp_path / "t")
    write_deltalake(p, pa.table({"id": list(range(10))}), mode="overwrite",
                    configuration={"delta.enableChangeDataFeed": "true"})
    DeltaTable(p).delete("id < 3")
    cdf = DeltaTable(p).load_cdf(starting_version=1).read_all()
    assert cdf.column("_change_type").to_pylist().count("delete") == 3


# ── iceberg (NB5) ──────────────────────────────────────────────────────────

@pytest.fixture
def iceberg_table(tmp_path, monkeypatch):
    monkeypatch.setattr(lh, "ROOT", tmp_path)
    monkeypatch.setattr(lh, "ICEBERG_ROOT", tmp_path / "iceberg")
    from pyiceberg.transforms import DayTransform

    cat = lh.catalog("test")
    ns = lh.namespace(cat, "t")
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False),
                        pa.field("ts", pa.timestamp("us"), nullable=False)])
    tbl = cat.create_table(f"{ns}.e", schema=schema)
    with tbl.update_spec() as spec:
        spec.add_field("ts", DayTransform(), "ts_day")
    tbl = cat.load_table(f"{ns}.e")
    for d in range(1, 9):
        tbl.append(pa.table({"id": list(range(d * 10, d * 10 + 10)),
                             "ts": [dtm.datetime(2026, 8, d)] * 10}, schema=schema))
    return cat, cat.load_table(f"{ns}.e")


def test_hidden_partitioning_prunes_on_the_source_column(iceberg_table):
    """The user filters `ts`; the engine derives `ts_day`. That is the feature."""
    _, tbl = iceberg_table
    all_files = len(list(tbl.scan().plan_files()))
    one_day = len(list(tbl.scan(
        row_filter="ts >= '2026-08-04T00:00:00' and ts < '2026-08-05T00:00:00'"
    ).plan_files()))
    assert all_files >= 8 and one_day == 1, (all_files, one_day)


def test_rename_is_metadata_only_and_keeps_field_id(iceberg_table):
    cat, tbl = iceberg_table
    before = {f.name: f.field_id for f in tbl.schema().fields}
    with tbl.update_schema() as upd:
        upd.rename_column("id", "event_id")
    tbl = cat.load_table("t.e")
    after = {f.name: f.field_id for f in tbl.schema().fields}
    assert after["event_id"] == before["id"]
    assert tbl.scan().to_arrow().num_rows == 80


def test_expire_snapshots_is_metadata_only(iceberg_table):
    """Measured behaviour NB6 teaches: expiry strands manifest lists on disk.

    Job 3 and Job 4 are a pair; if pyiceberg starts deleting the files itself,
    this fails and NB6's claim must be revisited.
    """
    cat, tbl = iceberg_table
    meta = Path(tbl.location().replace("file://", "")) / "metadata"
    avro_before = len(list(meta.glob("*.avro")))
    tbl.maintenance.expire_snapshots().by_ids(
        [s.snapshot_id for s in tbl.snapshots()[:-2]]).commit()
    tbl = cat.load_table("t.e")
    assert len(tbl.snapshots()) == 2
    assert len(list(meta.glob("*.avro"))) == avro_before, (
        "pyiceberg now deletes stranded manifests — update NB6's Job 3/4 pairing"
    )
    assert tbl.scan().to_arrow().num_rows == 80


# ── cross-notebook isolation (student simulation regression) ───────────────

def test_catalogs_are_isolated_per_name(tmp_path, monkeypatch):
    """NB5/NB6/NB8 and `make smoke` each reset their catalog on startup.

    They must not share a directory: students keep several notebooks open in
    Jupyter Lab, and a shared catalog turns that into an intermittent
    "corrupt install". Regression for the student-simulation S6 failure.
    """
    monkeypatch.setattr(lh, "ROOT", tmp_path)
    monkeypatch.setattr(lh, "ICEBERG_ROOT", tmp_path / "iceberg")

    schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    cat_a = lh.catalog("nb5")
    lh.namespace(cat_a, "lake")
    cat_a.create_table("lake.t", schema=schema).append(pa.table({"id": [1, 2, 3]},
                                                                schema=schema))

    # A second notebook starts up and resets ITS catalog.
    lh.reset_catalog("nb6")
    cat_b = lh.catalog("nb6")
    lh.namespace(cat_b, "lake")

    # The first notebook's table must be untouched.
    assert lh.catalog("nb5").load_table("lake.t").scan().to_arrow().num_rows == 3
    assert lh.reset_catalog("nb6") is None
    assert lh.catalog("nb5").load_table("lake.t").scan().to_arrow().num_rows == 3


def test_reset_catalog_does_not_touch_siblings(tmp_path, monkeypatch):
    monkeypatch.setattr(lh, "ROOT", tmp_path)
    monkeypatch.setattr(lh, "ICEBERG_ROOT", tmp_path / "iceberg")
    lh.catalog("keep")
    lh.catalog("drop")
    lh.reset_catalog("drop")
    assert (tmp_path / "iceberg" / "keep").exists()
    assert not (tmp_path / "iceberg" / "drop").exists()
