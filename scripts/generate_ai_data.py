"""Generate the AI-workload corpus used by NB7 (multimodal/vectors) and NB8 (agents/provenance).

Everything here is synthetic and **offline** — no model download, no API key.

Embeddings are built from a topic structure (centroid + noise) rather than
random noise, so nearest-neighbour search returns *semantically* sensible
results and recall is a meaningful number. A pure-random corpus would make
every retrieval metric in NB7 a coin flip.

Provenance columns (source / license / consent / subject_id) are carried from
the start because that is the actual lesson of NB8: provenance you did not
capture at ingest cannot be reconstructed later.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import numpy as np
import pyarrow as pa
from deltalake import write_deltalake

from lakehouse import ROOT, human, path, reset

DIM = 256
N_DOCS = 2_000
N_TOPICS = 8
BLOB_KB = 64
N_BLOBS = 200
NOISE_NORM = 0.6   # within-topic spread; see make_embeddings()
SEED = 42

TOPICS = ["inference", "training", "retrieval", "storage",
          "governance", "billing", "networking", "evaluation"]

# Provenance mixes: a realistic corpus is NOT uniformly licensed, and that is
# precisely why EU AI Act Art. 10 provenance records are hard to produce later.
# (source, license, consent_train, generator) — one row per EU AI Act Art. 10
# bucket, plus one deliberately unclassifiable source so the audit has teeth.
SOURCES = [
    ("internal_docs",  "proprietary", True,  None),          # → licensed
    ("vendor_feed",    "commercial",  True,  None),          # → licensed
    ("public_crawl",   "cc-by-4.0",   True,  None),          # → public domain
    ("user_uploads",   "user-owned",  True,  None),          # → scraped, opt-out checked
    ("model_output",   "synthetic",   True,  "claude-sonnet-4-6"),  # → synthetic
    ("scraped_forum",  "unknown",     False, None),          # → FAILS the audit
]


def make_embeddings(rng: np.random.Generator, n: int, dim: int, n_topics: int):
    """Topic-clustered unit vectors. Returns (embeddings, topic_ids)."""
    centroids = rng.normal(size=(n_topics, dim)).astype("float32")
    centroids /= np.linalg.norm(centroids, axis=1, keepdims=True)
    topic_ids = rng.integers(0, n_topics, size=n)
    # Scale noise by 1/sqrt(dim) so its NORM (not its per-component sigma) is
    # what we control. Per-component sigma=0.35 at dim=256 gives a noise vector
    # of norm ~5.6 against a unit centroid — the topic signal vanishes and the
    # corpus is effectively random. Norm 0.6 puts same-topic cosine at ~0.86.
    noise = rng.normal(scale=NOISE_NORM / np.sqrt(dim), size=(n, dim)).astype("float32")
    emb = centroids[topic_ids] + noise
    emb /= np.linalg.norm(emb, axis=1, keepdims=True)
    return emb, topic_ids


def make_corpus(n: int = N_DOCS, dim: int = DIM, seed: int = SEED) -> pa.Table:
    rng = np.random.default_rng(seed)
    emb, topic_ids = make_embeddings(rng, n, dim, N_TOPICS)

    src_idx = rng.integers(0, len(SOURCES), size=n)
    sources = [SOURCES[i][0] for i in src_idx]
    licenses = [SOURCES[i][1] for i in src_idx]
    consent = [bool(SOURCES[i][2]) for i in src_idx]
    # Art. 10 requires synthetic data to record what generated it.
    generators = [SOURCES[i][3] for i in src_idx]

    return pa.table({
        "doc_id":     pa.array(list(range(n)), pa.int64()),
        "title":      [f"{TOPICS[t]}-note-{i:05d}" for i, t in enumerate(topic_ids)],
        "topic":      [TOPICS[t] for t in topic_ids],
        "subject_id": pa.array([f"user_{i % 250:03d}" for i in range(n)], pa.string()),
        "source":     sources,
        "license":    licenses,
        "consent_train": pa.array(consent, pa.bool_()),
        "generator":    pa.array(generators, pa.string()),
        "blob_uri":   [f"blobs/frame_{i % N_BLOBS:04d}.bin" for i in range(n)],
        # The embedding travels in the SAME row as the data it describes.
        "emb": pa.FixedSizeListArray.from_arrays(pa.array(emb.ravel(), pa.float32()), dim),
    })


def make_blobs(out_dir: Path, n: int = N_BLOBS, kb: int = BLOB_KB, seed: int = SEED) -> int:
    """Simulated media frames. Random bytes so they do not compress away."""
    rng = np.random.default_rng(seed)
    out_dir.mkdir(parents=True, exist_ok=True)
    total = 0
    for i in range(n):
        blob = rng.integers(0, 256, size=kb * 1024, dtype=np.uint8).tobytes()
        (out_dir / f"frame_{i:04d}.bin").write_bytes(blob)
        total += len(blob)
    return total


def make_trajectories(n_sessions: int = 300, seed: int = SEED) -> pa.Table:
    """Agent runs: the lakehouse as system-of-record for what an agent DID.

    Shape mirrors an OpenTelemetry gen_ai span: one row per step, grouped by
    session, with the tool called, tokens burned, and whether the step failed.
    """
    rng = np.random.default_rng(seed + 1)
    tools = ["search_docs", "run_sql", "get_schema", "summarize", "write_report"]
    rows = []
    for s in range(n_sessions):
        n_steps = int(rng.integers(2, 9))
        outcome = rng.random() < 0.72          # 72% of sessions succeed
        for step in range(n_steps):
            failed = (not outcome) and step == n_steps - 1
            rows.append({
                "session_id": f"sess_{s:04d}",
                "step": step,
                "tool": tools[int(rng.integers(0, len(tools)))],
                "input_tokens": int(rng.integers(200, 4000)),
                "output_tokens": int(rng.integers(20, 900)),
                "latency_ms": int(rng.integers(150, 6000)),
                "status": "error" if failed else "ok",
                "reward": float(1.0 if outcome else 0.0),
                "subject_id": f"user_{s % 250:03d}",
            })
    return pa.Table.from_pylist(rows)


def main() -> None:
    docs = make_corpus()
    docs_path = path("bronze", "docs_multimodal")
    reset(docs_path)
    write_deltalake(docs_path, docs, mode="overwrite")

    blob_dir = Path(ROOT) / "blobs"
    blob_bytes = make_blobs(blob_dir)

    traj = make_trajectories()
    traj_path = path("bronze", "agent_traces")
    reset(traj_path)
    write_deltalake(traj_path, traj, mode="overwrite")

    print(f"docs      {docs.num_rows:,} rows, dim={DIM}  → {docs_path}")
    print(f"blobs     {N_BLOBS} files, {human(blob_bytes)}  → {blob_dir}")
    print(f"traces    {traj.num_rows:,} steps / 300 sessions → {traj_path}")
    print("\nReady for NB7 (multimodal + vectors) and NB8 (agents + provenance).")


if __name__ == "__main__":
    main()
