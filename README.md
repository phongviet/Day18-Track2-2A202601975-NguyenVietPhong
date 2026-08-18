# Day 18 — Lakehouse Lab (Track 2)

Lab cho **AICB-P2T2 · Ngày 18 · Data Lakehouse Architecture**.

Tám notebook, hai nửa:

* **NB1–NB4 — nền tảng.** Delta Lake ACID, OPTIMIZE/Z-ORDER, time travel, medallion Bronze→Silver→Gold.
* **NB5–NB8 — lakehouse 2026.** Iceberg và **catalog như control plane**, 4 job maintenance bắt buộc, multimodal + vector trong bảng, agent trajectory + provenance (EU AI Act Art. 10).

Tất cả chạy **offline**: không API key, không Docker, không JVM, không tải model, không tải DuckDB extension.

---

## Quick Start

```bash
git clone https://github.com/VinUni-AI20k/Day18-Track2-Lakehouse-Lab.git
cd Day18-Track2-Lakehouse-Lab
make setup      # ~20s pip / ~4s uv
make smoke      # ~5s  — 9 checks, hoàn toàn offline
make data       # Bronze cho NB4
make data-ai    # corpus multimodal + agent traces cho NB7/NB8
make lab        # http://localhost:8888
```

Yêu cầu: **Python 3.10 – 3.14**. Không cần gì khác.

> **Đã sửa (v2):** phiên bản trước chặn Python 3.14 vì `pyarrow` chưa có wheel.
> Toàn bộ stack nay đã có wheel 3.14 — kiểm chứng ngày 2026-08-17 trên cả 3.12 và 3.14.

Kiểm tra mọi thứ chạy được trước khi nộp:

```bash
make test       # 22 pytest, ~1s
make run-all    # chạy cả 8 notebook headless, ~10s
```

---

## Tám notebook

| NB | Chủ đề | Bạn **đo** được gì | Slide |
|---|---|---|---|
| `01_delta_basics` | Transaction log, schema enforcement + evolution | `_delta_log/` JSON; bad write bị chặn; `tier` thêm khi opt-in | §2 |
| `02_optimize_zorder` | Small files, OPTIMIZE + Z-ORDER | speedup ≥ 3× **hoặc** files-pruned ≥ 10× | §6 |
| `03_time_travel` | `versionAsOf`, MERGE, RESTORE | `history()` ≥ 5 version kể cả RESTORE | §3 |
| `04_medallion` | Bronze→Silver→Gold cho LLM observability | Silver < Bronze (dedup); Gold p50/p95/cost ≥ 7 ngày | §8 |
| `05_iceberg_catalog` | **Iceberg + catalog là control plane** | hidden-partition pruning ≥ 5×; field-ID bền qua rename; 2 partition spec cùng tồn tại | §4, §12 |
| `06_maintenance` | **4 job bắt buộc** + job thứ 5 | compaction ≥ 10× ít file; clustering skip ≥ 50%; orphan + snapshot expiry | §6, §12 |
| `07_vectors_multimodal` | Blob inline vs pointer; embedding trong bảng | amplification khi random-read; int8 nhỏ 4×; **lifecycle bug** tái hiện được | §11 |
| `08_agents_provenance` | Trajectory, MCP 2026-07-28, provenance | pin version cho training run; 4 rổ Art. 10 thành partition | §11, §12 |

Mỗi notebook tự kết thúc bằng một khối `assert` trên tiêu chí đậu — `make run-all` vì thế là **cùng một cổng** mà giảng viên chạy khi chấm.

---

## Ba điều notebook đo được mà slide chưa nói

Lab này không chỉ minh hoạ slide. Ba kết quả dưới đây là **đo thật trên máy bạn**, và đều là bẫy production:

1. **`VACUUM` không dọn orphan chưa từng commit.** `deltalake` (Rust/Python) chỉ thu hồi file đã bị *tombstone* trong log. File do job crash để lại chưa từng vào log → vô hình với vacuum ở mọi retention. NB6 đo, rồi bắt bạn tự viết phép hiệu tập hợp.
2. **`expire_snapshots` của Iceberg chỉ đụng metadata.** 20 → 3 snapshot nhưng **0 file avro bị xoá**; metadata còn *phình ra*. Job 3 và Job 4 là một **cặp** — chạy expiry mà không quét orphan là lý do "đã expire mà hoá đơn S3 không giảm".
3. **Delta không có kiểu vector cố định chiều.** `fixed_size_list<float>[256]` ghi xuống rồi đọc lên thành `list<float>`; phải cast lúc query. Đó chính là lý do Hudi 1.2 thêm cột `VECTOR(dim, type)` hạng nhất.

Hai điều đầu được "ghim" bằng test canary trong `tests/` — nếu thư viện đổi hành vi, test đỏ và notebook phải sửa theo.

---

## Lệnh `make`

```
make setup     Tạo venv + cài deps (~180 MB)
make smoke     9 check offline (~5s)
make test      22 pytest (~1s)
make data      Bronze 200K dòng cho NB4
make data-ai   Corpus multimodal + agent traces cho NB7/NB8
make run-all   Chạy cả 8 notebook headless — cổng chấm điểm
make simulate  Mô phỏng 12 kịch bản học viên (SIM_FAST=1 để bỏ 2 kịch bản dựng venv)
make lab       Mở Jupyter Lab
make clean     Xoá venv + _lakehouse/

make spark-up / spark-smoke / spark-data / spark-down / spark-clean
               Đường Spark/Docker tuỳ chọn (chỉ phủ NB1–NB4)
```

**Notebook lưu dạng Jupytext `.py`** (nhẹ, dễ review). `make setup` / `make lab` tự sinh `.ipynb`; sửa trong Jupyter, Jupytext đồng bộ ngược lại.

---

## Hai đường chạy

| Path | Stack | Setup | RAM | Phủ |
|---|---|---|---|---|
| **Lightweight (mặc định)** | `deltalake` 1.x + `pyiceberg` + DuckDB + Polars | `make setup`, ~20 s | ~600 MB | **cả 8 NB** |
| **Spark (Docker Compose)** | PySpark 3.5 + delta-spark + MinIO | `make spark-up`, ~3–8 phút | ~6 GB | 4 NB PySpark **+ cả 8 NB lightweight** |
| **Spark (Apple `container`)** | y hệt trên, chạy bằng `container run` | `make apple-up`, ~3–8 phút | ~6 GB | y hệt trên |

Cả hai ghi ra **cùng định dạng Delta trên đĩa** — đổi qua lại lúc nào cũng đọc được.
Container Spark nay cài cả stack lightweight, nên bạn chạy được **cả 12 notebook**
trong đó (4 bản PySpark ở `notebooks-spark/` + 8 bản lightweight ở `notebooks/`).

> **Đường Spark đã được kiểm chứng đầu-cuối 17/8/2026** (Docker qua lima, rootless):
> `verify.py` xanh (Spark→MinIO→Delta→time travel), `generate_data.py` ghi 1 triệu dòng,
> 4/4 notebook PySpark và 8/8 notebook lightweight chạy được trong container.
> Trước đó đường này **hỏng hoàn toàn** — xem mục dưới.

---

### Chạy Spark bằng Apple `container` (macOS 15+, Apple silicon)

[`apple/container`](https://github.com/apple/container) **không chạy được**
`docker-compose.yml`: nó không có compose plugin (`container compose` →
*Plugin 'container-compose' not found*) và **không expose Docker API socket**,
nên cả `docker` lẫn `docker compose` đều không điều khiển được.

`scripts/apple_container.sh` dựng **đúng stack 3 service đó** bằng `container run`.
File compose **giữ nguyên** — hai đường song song, chọn cái bạn có:

```bash
brew install container
container system kernel set --recommended   # bắt buộc: `system start` sẽ hỏi và treo nếu không có TTY
container system start

make apple-up       # MinIO + buckets + Spark/Jupyter
make apple-smoke    # scripts/verify.py trong container
make apple-data     # sinh Bronze 1 triệu dòng bằng Spark
make apple-status   # xem container + IP của MinIO
make apple-down     # dừng (giữ dữ liệu MinIO)  ·  apple-clean = xoá luôn
```

**Khác biệt kỹ thuật duy nhất:** Compose phân giải tên service `minio` qua DNS
nội bộ. Apple `container` **không phân giải tên** trừ khi bạn tạo DNS domain, mà
`container system dns create` **cần sudo**. Nên script đọc IP của MinIO bằng
`container inspect` rồi truyền vào biến `MINIO_ENDPOINT`;
`scripts/spark_session.py` đọc biến này và **mặc định vẫn là
`http://minio:9000`** — đường compose không đổi hành vi.

---

## Deliverable

Nộp 8 notebook đã chạy (giữ output) + ảnh chụp. Chi tiết thang điểm: [`rubric.md`](rubric.md) — 100 điểm → Track-2 Daily Lab (30%).

1. **NB1** — `_delta_log/` JSON; bad-schema write bị chặn; `schema_mode="merge"` thêm cột `tier`
2. **NB2** — speedup ≥ 3× **hoặc** files-pruned ≥ 10×
3. **NB3** — MERGE 100K + RESTORE; `history()` ≥ 5 version *sau* restore
4. **NB4** — Bronze/Silver/Gold trên đĩa; Silver < Bronze; Gold ≥ 7 ngày × 3 model
5. **NB5** — pruning ratio ≥ 5× khi lọc trên `ts`; `latency_millis` giữ nguyên `field_id`; ≥ 2 `spec_id`
6. **NB6** — 4 job chạy đủ, kèm số trước/sau; 3 orphan tìm và xoá được
7. **NB7** — amplification random-read; int8 nhỏ ≥ 3×; **tái hiện lifecycle bug** (external index còn trả dữ liệu đã xoá)
8. **NB8** — Silver partition theo `agent_version`; replay đúng version đã pin; 4 rổ Art. 10 thành partition

Ngoài ra: `submission/REFLECTION.md` (≤ 200 từ) — trong "Top 5 Lakehouse Anti-Patterns", team bạn dễ vướng cái nào nhất, vì sao?

---

## Bonus Challenge (tuỳ chọn, không tính điểm)

Một **architecture brief** mở: chọn một bài toán dữ liệu khó thật (LLM observability 1B req/ngày, CDC tuân thủ Nghị định 13, corpus nghìn tỷ token, multimodal RAG, tiering chặn trần FinOps, migration catalog…) và thiết kế chiến lược lưu trữ bạn dám bảo vệ trong design review.

Tài liệu là deliverable; code tuỳ chọn. Bài nộp được nhận xét viết tay, tập trung vào **phán đoán**: có nêu phương án đã loại và lý do không? Số liệu có thực tế không? Xem [`BONUS-CHALLENGE.md`](BONUS-CHALLENGE.md) (VI) · [`BONUS-CHALLENGE-EN.md`](BONUS-CHALLENGE-EN.md) (EN).

---

## Cấu trúc repo

```
.
├── Makefile · README.md · rubric.md
├── requirements.txt          # lightweight: deltalake 1.x, pyiceberg, duckdb, polars, numpy
├── pytest.ini
├── notebooks/                # ← đường lightweight (mặc định)
│   ├── 01_delta_basics.py        05_iceberg_catalog.py
│   ├── 02_optimize_zorder.py     06_maintenance.py
│   ├── 03_time_travel.py         07_vectors_multimodal.py
│   └── 04_medallion.py           08_agents_provenance.py
├── notebooks-spark/          # NB1–NB4 bản PySpark
├── scripts/
│   ├── lakehouse.py              # path/catalog/đo đạc helper
│   ├── generate_data_lite.py     # Bronze LLM-observability (NB4)
│   ├── generate_ai_data.py       # corpus multimodal + trajectory (NB7/NB8)
│   ├── verify_lite.py            # make smoke
│   ├── run_all.py                # make run-all
│   └── spark_session.py · generate_data.py · verify.py
├── tests/test_lab18.py       # make test
└── docker/docker-compose.yml
```

---

## Troubleshooting

| Triệu chứng | Fix |
|---|---|
| `make setup` báo `python3: command not found` | Cài Python 3.10–3.14 hoặc `uv` |
| `AttributeError: 'DeltaTable' object has no attribute 'files'` | Bạn đang ở `deltalake` 0.x. `make clean && make setup` (lab dùng 1.x, `file_uris()`) |
| `No function matches array_cosine_similarity(FLOAT[], …)` | Thiếu cast: `emb::FLOAT[256]`. Delta trả về list biến chiều — xem ghi chú trong NB7 |
| NB2 speedup < 3× | Bình thường khi RAM thấp; tiêu chí cho phép dùng files-pruned ≥ 10× thay thế |
| Quên `make data` / `make data-ai` | Không sao — NB4/NB7/NB8 tự sinh dữ liệu thiếu khi chạy |
| Mở nhiều notebook cùng lúc trong Jupyter | An toàn: NB5/NB6/NB8 và `make smoke` mỗi cái dùng **catalog Iceberg riêng** |
| Máy chặn mạng hoàn toàn | Vẫn chạy được. Nếu gặp lỗi tải extension, bạn đang gọi `delta_scan()` — lab dùng Arrow thay thế |

---

## Submission

Fork repo → push 8 notebook đã chạy + `submission/REFLECTION.md` → PR về upstream, title `[NXX] Lab18 — <Họ Tên>`.

---

## Đã kiểm thử như thế nào

Ngoài `make test` / `make run-all`, lab được chạy qua một bộ **mô phỏng học viên** 12 kịch bản —
những thứ học viên làm mà tác giả không làm:

chạy notebook **ngược thứ tự** · chạy lại lần hai · **quên `make data`** · cwd là `notebooks/`
(mặc định của Jupyter) · **mở 2 notebook cùng lúc** · chạy `make smoke` khi notebook đang chạy ·
**mất mạng hoàn toàn** · máy đang tải nặng CPU · thực thi `.ipynb` qua `nbconvert` ·
`make clean` giữa chừng · **Python 3.10** (cũ nhất) · **`pip` thuần, không `uv`**.

Chạy lại bất cứ lúc nào: `make simulate`. Hai lỗi thật đã tìm ra và sửa từ bộ này: NB4 chết với lỗi Rust thô khi thiếu Bronze (nay tự sinh),
và `make smoke` xoá mất catalog của notebook đang chạy (nay mỗi notebook một catalog riêng).
Cả hai đều có test hồi quy trong `tests/`.

### Đường Spark: 4 lỗi có sẵn, nay đã sửa

Lần đầu thực sự khởi động Docker cho lab này (17/8/2026) lộ ra rằng đường Spark
**chưa từng chạy được** trên Docker Compose hiện đại. Bốn lỗi độc lập, tất cả đều
có từ trước:

1. **Compose interpolation** — `${f%.py}` trong khối `command:` bị Compose hiểu là
   biến, báo `invalid interpolation format` và **stack không lên được**. Phải nhân
   đôi dấu `$` (kể cả trong dòng *comment* — Compose nội suy cả comment).
2. **`jupytext: command not found` (exit 127)** — `pip install --user` đặt script vào
   `~/.local/bin`, không nằm trong PATH; `set -e` giết container trước khi Jupyter chạy.
3. **`PYTHONPATH` bị ghi đè** — image này expose `pyspark` *chỉ* qua `PYTHONPATH`;
   compose đặt `PYTHONPATH: /workspace/scripts` nên **`import pyspark` hỏng ở mọi nơi**.
   Nay nối thêm đường dẫn Spark của image thay vì thay thế.
4. **Ivy cache không ghi được** — named volume gắn ở `~/.ivy2` (đường dẫn *không có*
   trong image) nên Docker tạo nó thuộc `root`; Ivy chết khi resolve `delta-spark`,
   JVM thoát trước khi Py4J gateway lên (`JAVA_GATEWAY_EXITED`). Nay trỏ
   `spark.jars.ivy` vào `~/.cache/ivy` — thư mục *có* trong image nên volume thừa
   kế quyền của `jovyan`.

Ngoài ra việc convert `.py`→`.ipynb` nay là *best-effort*: trên host Linux mà UID
khác 1000, bind mount không ghi được — trước đây điều đó giết container, giờ chỉ
in cảnh báo và Jupyter vẫn lên.

---

© VinUniversity AICB program. Bám sát Track 2 Day 18 slide (55 trang, bản 8/2026).
