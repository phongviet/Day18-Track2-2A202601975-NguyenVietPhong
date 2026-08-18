# Day 18 Lab — Grading Rubric (100 pts)

Track-2 Daily Lab weight = 30%.

Every criterion below is **machine-checkable**: each notebook ends in an
`assert` block over its own pass criteria, and `make run-all` executes all
eight. A submission where `make run-all` is green has satisfied the
mechanical half of the rubric by construction — the remaining judgement is
whether the student can *explain* the numbers.

```bash
make setup && make smoke && make data && make data-ai && make test && make run-all
```

Two paths are supported for NB1–NB4 (lightweight `deltalake` vs Spark/Docker).
Both write the **same on-disk Delta format**, so evidence from either counts:

* *MinIO `_delta_log/` visible* ↔ *`_lakehouse/.../_delta_log/` on local disk*
* *Spark `OPTIMIZE … ZORDER BY`* ↔ *`dt.optimize.compact()` + `dt.optimize.z_order(...)`*
* *Spark `MERGE INTO`* ↔ *`dt.merge(...).when_matched_update_all().execute()`*

NB5–NB8 are lightweight-path only (`pyiceberg` is pure Python).

---

## Part A — Foundations (44 pts)

| # | Notebook | Criterion | Pts |
|---|---|---|---:|
| 1 | `01_delta_basics` | Delta table created; `_delta_log/` JSON commits visible | 4 |
| 1 | `01_delta_basics` | Schema enforcement blocks the `age=str` write | 2 |
| 1 | `01_delta_basics` | `schema_mode="merge"` adds the `tier` column (opt-in evolution) | 2 |
| 2 | `02_optimize_zorder` | Small-file problem reproduced (≥ 100 files before OPTIMIZE) | 3 |
| 2 | `02_optimize_zorder` | Speedup ≥ 3× **or** files-pruned ratio ≥ 10× | 6 |
| 2 | `02_optimize_zorder` | `numFiles` drops meaningfully after OPTIMIZE | 3 |
| 3 | `03_time_travel` | `history()` shows ≥ 5 versions **including the RESTORE row** | 4 |
| 3 | `03_time_travel` | MERGE upsert 100K rows succeeds | 4 |
| 3 | `03_time_travel` | RESTORE rolls back bad data; `score < 0` count = 0 | 4 |
| 4 | `04_medallion` | Bronze, Silver, Gold all present on the storage layer | 4 |
| 4 | `04_medallion` | Silver dedup measurably drops rows (Silver < Bronze) | 4 |
| 4 | `04_medallion` | Gold correct (p50/p95, cost_usd, error_rate) for ≥ 7 dates × 3 models | 4 |
|   | | **Part A subtotal** | **44** |

## Part B — Lakehouse 2026 (50 pts)

| # | Notebook | Criterion | Pts |
|---|---|---|---:|
| 5 | `05_iceberg_catalog` | Table created **through the catalog**; partition spec uses `day(ts)` | 3 |
| 5 | `05_iceberg_catalog` | Hidden-partition pruning ≥ 5× measured via `plan_files()`, filtering on `ts` (not `ts_day`) | 5 |
| 5 | `05_iceberg_catalog` | Three-tier metadata walked; metadata:data byte ratio reported | 1 |
| 5 | `05_iceberg_catalog` | Rename keeps `field_id` (metadata-only); ≥ 2 partition specs coexist and the table still reads | 4 |
| 6 | `06_maintenance` | **Job 1** Compaction: ≥ 10× fewer files, before/after reported | 4 |
| 6 | `06_maintenance` | **Job 2** Clustering: ≥ 50% of files skippable for a point query, proven from min/max stats | 3 |
| 6 | `06_maintenance` | **Job 3** Expiry: Delta vacuum reclaims bytes; Iceberg drops to 3 snapshots | 3 |
| 6 | `06_maintenance` | **Job 4** Orphans: 3 planted Delta orphans found + removed; stranded Iceberg manifest lists swept | 2 |
| 6 | `06_maintenance` | **Job 5** Checkpoint written (`*.checkpoint.parquet` + `_last_checkpoint`) | 1 |
| 7 | `07_vectors_multimodal` | Random-access amplification measured (≥ 5×) and explained via row-group granularity | 4 |
| 7 | `07_vectors_multimodal` | int8 quantization ≥ 3× smaller on disk; recall@10 **and** topic fidelity both reported | 4 |
| 7 | `07_vectors_multimodal` | Semantic search runs as SQL and returns on-topic neighbours | 1 |
| 7 | `07_vectors_multimodal` | **Lifecycle bug reproduced**: 0 hits in-table, > 0 hits in the stale external index | 4 |
| 8 | `08_agents_provenance` | Trajectories through medallion; Silver partitioned by `agent_version`; Gold covers both policies | 3 |
| 8 | `08_agents_provenance` | Training run pins the table version; replay at that version matches exactly | 3 |
| 8 | `08_agents_provenance` | MCP surface: cacheable `tools/list` (5 turns → 1 catalog read), `input_required` before destructive calls, task poll completes | 3 |
| 8 | `08_agents_provenance` | All **four** Art. 10 buckets exist as partitions; UNCLASSIFIED rows excluded from the trainable set | 2 |
|   | | **Part B subtotal** | **50** |

## Part C — Reproducibility (6 pts)

| Criterion | Pts |
|---|---:|
| `make test` green (22 tests) | 2 |
| `make run-all` green from a clean `make setup` | 4 |
| | **6** |

**Total: 100**

---

## What earns the top band

Full marks on a criterion require the number **and** the reading of it. Two
submissions can both print `pruning ratio: 10×` and only one has done the lab:

* *Adequate:* "Pruning ratio was 10×."
* *Strong:* "10× because the filter is on `ts` and Iceberg derived `ts_day`
  from the stored transform — a Hive user who forgot the partition predicate
  would have read all 10 files, ~$220/day at 10K queries."

NB6 and NB7 each contain a **measured finding that contradicts a common
belief** (`VACUUM` misses uncommitted orphans; `expire_snapshots` deletes no
files). A submission that notices and explains one of these demonstrates it
actually read its own output.

## Submission

Fork to `<your-username>/Day18-Track2-Lakehouse-Lab`, open a PR upstream with:

1. Eight executed notebooks (output cells preserved)
2. `submission/screenshots/` — at least one of:
   * MinIO console showing `_delta_log/` + bucket layout (Spark path), **or**
   * `tree _lakehouse/` plus the contents of one `_delta_log/*.json` (lightweight path)
3. `submission/REFLECTION.md` (≤ 200 words): which anti-pattern from the
   "Top 5 Lakehouse Anti-Patterns" slide is your team's data most at risk of, and why?
4. *Optional:* `submission/bonus/ARCHITECTURE.md` for the
   [bonus challenge](BONUS-CHALLENGE.md) — reviewed, ungraded.

## Late policy / regrade

Standard Track-2 policy applies — see `INDEX-Track2.md`.
