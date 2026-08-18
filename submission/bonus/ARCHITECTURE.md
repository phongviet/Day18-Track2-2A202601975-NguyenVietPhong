# KIẾN TRÚC LAKEHOUSE CHO RIDE-HAILING VIỆT NAM (TUÂN THỦ NGHỊ ĐỊNH 13/2023/NĐ-CP)

**Tác giả:** Nguyễn Viết Phong  
**Dự án:** Vietnam Urban Mobility Lakehouse Platform (V-Ride)  
**Ngày:** 18/08/2026  
**Trạng thái:** Proposed (Ready for Senior Architecture Review)

---

## 1. Problem Statement

Hệ thống điều vận và gọi xe công nghệ (V-Ride) xử lý **100 triệu chuyến xe/năm** (~274.000 chuyến/ngày, đạt đỉnh **30.000 writes/giây** vào giờ cao điểm mưa bão/tan tầm) từ cơ sở dữ liệu giao dịch Oracle OLTP. 

Thách thức cốt lõi gồm 3 trục:
1. **Độ trễ & Tính nhất quán**: SLA làm tươi Dashboard quản trị trong **≤ 60 giây**, độ trễ truy vấn phân tích $p_{95} < 1.0\text{ s}$ trên dữ liệu 90 ngày. Dữ liệu đến muộn (Late-arriving data) do tài xế mất sóng 4G tại các tỉnh thành xảy ra liên tục (out-of-order lên tới 6–12 giờ).
2. **Tuân thủ Pháp lý Nghiêm ngặt (Nghị định 13/2023/NĐ-CP)**: Toàn bộ dữ liệu PII tài xế/khách hàng (Số điện thoại, CCCD/CMND, GPS tọa độ từng giây, biển số xe, thẻ thanh toán) phải được mã hóa/token hóa ngay tại tầng Bronze, hỗ trợ Quyền được xóa dữ liệu (Right-to-be-Forgotten - Điều 17) và ghi vết toàn bộ nhật ký truy cập (Access Audit Log - Điều 11).
3. **Hiệu năng & FinOps**: Quản lý lưu trữ ~120 TB/năm với chi phí lưu trữ & xử lý tối ưu dưới **\$2.500/tháng**.

---

## 2. Architecture Diagram

```
                 [ V-RIDE OLTP & TELEMETRY SOURCES ]
  +----------------------+      +--------------------------------+
  | Oracle Production DB |      | Driver Mobile Apps / IoT (GPS) |
  | (Trips, Users, Pay)  |      | (MQTT / TCP Telemetry Stream)  |
  +----------+-----------+      +---------------+----------------+
             | (Oracle LogMiner)                |
             v                                  v
     +---------------+                  +---------------+
     | Debezium CDC  |                  | Kafka Ingest  |
     | Kafka Connect |                  | (Raw GPS Pin) |
     +-------+-------+                  +-------+-------+
             |                                  |
             +----------------+-----------------+
                              v
                  [ APACHE KAFKA CLUSTER ]
                   (Topic: cdc.vride.raw)
                              |
                              | Streaming Read (Micro-batch 30s)
                              v
==================== LAKEHOUSE CORE ENGINE ====================
|  +--------------------------------------------------------+ |
|  | SPARK STRUCTURED STREAMING INGESTION ENGINE            | |
|  | - Schema Validation & Dead Letter Queue (DLQ)          | |
|  | - Deterministic HMAC-SHA256 Tokenizer via HashiCorp    | |
|  |   Vault / AWS KMS (Secret Salt Key Rotation)           | |
|  +---------------------------+----------------------------+ |
|                              |                              |
|                              v                              |
|  [ BRONZE TIER ] (Raw Append-Only + Tokenized PII)         |
|  - Table: `bronze.ride_events_raw`                         |
|  - Storage: S3 Standard / MinIO Storage                    |
|  - Format: Delta Lake 3.2+ (CDF Enabled, Daily Partition)  |
|  - PII Columns: `phone_token`, `cccd_token`, `gps_obf`     |
|                              |                              |
|                              | Spark Structured Streaming   |
|                              | Delta MERGE with Monotonic TS|
|                              v                              |
|  [ SILVER TIER ] (Cleaned, Deduplicated, SCD Type 2)       |
|  - Tables: `silver.trips_scd2`, `silver.driver_locations`  |
|  - Deduplication key: `trip_id + event_sequence`           |
|  - Partitioning: `date(trip_start_ts)` + Liquid Clustering |
|  - Deletion Vectors: Enabled (Sub-second ACID deletes)     |
|                              |                              |
|                              | Scheduled Compaction / dbt   |
|                              | Aggregations every 5 mins    |
|                              v                              |
|  [ GOLD TIER ] (Business Aggregates, Compliance & BI)      |
|  - `gold.driver_hourly_performance` (KPI, Earnings)        |
|  - `gold.route_demand_heatmaps` (H3 Hexagon Geo-analytics) |
|  - `gold.compliance_audit_access_log` (Decree 13 Art. 11)   |
|                              |                              |
===============================================================
                               |
            +------------------+------------------+
            v                                     v
[ REAL-TIME ANALYTICS & BI ]             [ AUDIT & COMPLIANCE ]
- Trino / DuckDB / StarRocks             - Legal & Security Team
- Apache Superset / Metabase             - Data Subject Request (DSR)
- Latency p95 < 800ms                    - Right-to-be-Forgotten Service
```

---

## 3. Quyết định Kiến trúc & Phân tích Đánh đổi (Alternatives Eliminated)

### Quyết định 1: Table Format — Chọn Delta Lake 3.2+ với Deletion Vectors & Change Data Feed (CDF)
* **Lựa chọn:** **Delta Lake 3.2+**.
* **Lý do chọn:** Hỗ trợ Change Data Feed (CDF) native cực mạnh cho streaming downstream, và **Deletion Vectors (DVs)** cho phép xoá/cập nhật từng dòng (SCD Type 2 & Yêu cầu xoá PII theo Điều 17 Nghị định 13) mà không cần viết lại toàn bộ file Parquet 128 MB.
* **Lựa chọn bị loại:**
  1. *Apache Iceberg (Loại)*: Iceberg v2 hỗ trợ Position/Equality deletes rất tốt, nhưng tại thời điểm 2026 trên hệ sinh thái Spark Streaming CDC, Delta CDF có độ trễ commit thấp hơn (~15% throughput cao hơn cho 30k writes/s micro-batches) và tích hợp Deletion Vector ổn định hơn.
  2. *Vanilla Parquet trên Hive Metastore (Loại)*: Không có ACID, không có Time Travel, cập nhật CDC yêu cầu ghi đè nguyên partition, vi phạm hoàn toàn SLA 60s và làm hỏng tính toàn vẹn dữ liệu khi có concurrent reader.

### Quyết định 2: Chiến lược Xử lý PII — Tokenization một chiều bằng HMAC-SHA256 với Salt Vault tại Bronze Landing
* **Lựa chọn:** **Deterministic Tokenization tại Bronze Entrypoint**. Toàn bộ số điện thoại (`phone_number`), số định danh (`cccd`), số thẻ được thay thế bằng token `hmac_sha256(val, secret_salt)` ngay khi Spark đọc từ Kafka trước khi ghi xuống S3 Bronze. Bảng ánh xạ khóa (Key-Vault Table) được lưu tách biệt trong HSM/Vault có phân quyền MFA cấp cao.
* **Lý do chọn:** Đảm bảo dữ liệu nằm trên S3 Data Lake ở mọi tầng (Bronze, Silver, Gold) đều là dữ liệu đã phi danh tính hóa (Pseudonymized). Kể cả khi file Parquet bị lộ, dữ liệu cá nhân vẫn an toàn tuyệt đối, tuân thủ Điều 13 Nghị định 13.
* **Lựa chọn bị loại:**
  1. *Plaintext Storage kết hợp Row-Level Security (RLS) ở Query Engine (Loại)*: Quá nguy hiểm. File vật lý trên S3 vẫn chứa PII thô; nếu Data Scientist copy file raw hoặc truy cập trực tiếp qua S3 API thì toàn bộ RLS bị vượt qua (bypassed).
  2. *Dynamic Data Masking khi đọc (Loại)*: Tốn compute CPU giải mã/che mờ trên mỗi câu query, làm tăng độ trễ truy vấn ($p_{95}$ vọt lên > 3 giây) và không đáp ứng được tiêu chuẩn Storage-at-Rest compliance.

### Quyết định 3: Xử lý Out-of-Order & Dữ liệu Đến muộn — Idempotent MERGE với Monotonic Event Timestamp
* **Lựa chọn:** **MERGE INTO với điều kiện phiên bản thời gian nguồn (`src.event_ts > tgt.event_ts`)**. Khi tài xế đi vào vùng mất sóng (hầm thủ thiêm, vùng sâu vùng xa), các gói tin cập nhật trạng thái chuyến xe (`PICKING_UP`, `IN_TRIP`, `COMPLETED`) bị delay hàng giờ. Khi 4G kết nối lại, Kafka nhận loạt event này sau các event mới hơn.
  ```sql
  MERGE INTO silver.trips_scd2 AS tgt
  USING cdc_stream_microbatch AS src
  ON tgt.trip_id = src.trip_id
  WHEN MATCHED AND src.event_ts > tgt.last_updated_ts THEN
    UPDATE SET tgt.status = src.status, tgt.last_updated_ts = src.event_ts, ...
  WHEN NOT MATCHED THEN
    INSERT *
  ```
* **Lý do chọn:** Đảm bảo tính nhất quán cuối cùng (Eventual Consistency) và ngăn chặn việc trạng thái cũ ghi đè lên trạng thái mới hơn.
* **Lựa chọn bị loại:**
  1. *Blind Upsert (Cứ đến là ghi đè) (Loại)*: Sẽ làm trạng thái chuyến xe bị thụt lùi (ví dụ: Chuyến xe đã `COMPLETED` bị gói tin trễ ghi đè ngược lại thành `IN_TRIP`), gây sai lệch doanh thu và đối soát tài xế.
  2. *Chờ đợi toàn bộ qua Watermark 24h (Loại)*: Gây trễ bộ nhớ đệm (State Store) khổng lồ trên Spark Streaming, làm crash JVM do Out-Of-Memory (OOM) khi giữ state hàng triệu chuyến xe mở.

### Quyết định 4: Cơ chế Phân vùng & Clustering — Phân vùng theo Ngày kết hợp Liquid Clustering trên `(tenant_id, driver_id, h3_cell)`
* **Lựa chọn:** Partition theo `date(trip_start_ts)` cho tầng Silver, kết hợp **Liquid Clustering / Z-Ordering** trên các thuộc tính lọc điểm (`driver_id`, `h3_grid_idx`).
* **Lý do chọn:** Các câu truy vấn vận hành 99% đều lọc theo ngày + khu vực địa lý (H3 Spatial Grid) hoặc mã tài xế. Liquid Clustering giúp Data Skipping loại bỏ ≥ 92% số file Parquet không cần thiết, đạt $p_{95} < 500\text{ ms}$.
* **Lựa chọn bị loại:**
  1. *Phân vùng phân cấp sâu Hive-style `year/month/day/city/service_type` (Loại)*: Gây ra thảm họa **Small-File Problem** (sinh ra hàng triệu partition rỗng/vài KB), làm nghẽn Driver khi list metadata và tăng chi phí S3 ListObjects lên gấp 20 lần.

### Quyết định 5: Quản trị Vòng đời & Xóa Dữ liệu (Decree 13 Right-to-be-Forgotten) — Automated Tombstoning & Purge Pipeline
* **Lựa chọn:** Khi nhận Data Subject Request (DSR) yêu cầu xoá dữ liệu từ khách hàng:
  1. Xóa khóa token trong Key-Vault/HSM (khiến toàn bộ dữ liệu lịch sử của khách hàng đó trở thành ẩn danh vĩnh viễn - Crypto-shredding).
  2. Phát lệnh `DELETE FROM silver.trips_scd2 WHERE user_token = '...'` (tạo Deletion Vector tức thì trong 200 ms).
  3. Job `VACUUM RETAIN 168 HOURS` dọn sạch file vật lý định kỳ sau 7 ngày.
* **Lựa chọn bị loại:**
  1. *Chạy VACUUM RETAIN 0 HOURS ngay lập tức (Loại)*: Phá vỡ khả năng Time Travel phục vụ audit và làm crash các stream reader đang đọc bảng dở dang.

---

## 4. Ba Kịch Bản Sự Cố Lúc 3 Giờ Sáng (Failure Modes & Recovery)

### Kịch bản 1: Mạng chập chờn gây dồn ứ Kafka Lag & Đột biến Out-of-order 500.000 events/giây
* **Hiện tượng lúc 3h sáng**: Cáp quang biển bị nghẽn, các trạm BTS ngoại thành khôi phục kết nối đồng loạt đẩy hàng triệu event delay từ chiều hôm trước vào Kafka. Spark Streaming lag vọt từ 5 giây lên 45 phút, CPU container chạm 100%.
* **Phát hiện (Detection)**:
  - Alert Prometheus/Grafana: `kafka_consumer_lag_records > 100,000` trong 3 phút liên tục.
  - SLA Breach Alert: `lakehouse_ingestion_delay_seconds > 120s`.
* **Quy trình Khắc phục (Mitigation & Rollback)**:
  1. Tự động kích hoạt **Spark Dynamic Allocation** mở rộng worker node từ 4 lên 16 executors.
  2. Bật cờ `spark.sql.streaming.backpressure.enabled=true` và giới hạn `maxOffsetsPerTrigger=50000` để bảo vệ heap memory không bị OOM.
  3. Nhờ cơ chế `MERGE ... WHERE src.event_ts > tgt.last_updated_ts`, toàn bộ 500.000 event trễ được hòa nhập chuẩn xác mà không gây sai lệch dữ liệu tài chính.

### Kịch bản 2: DBA Oracle OLTP đổi kiểu dữ liệu cột mà không thông báo (Schema Drift)
* **Hiện tượng lúc 3h sáng**: Đội Backend OLTP cập nhật bảng `trips`, mở rộng cột `surge_multiplier` từ số thực `FLOAT` sang chuỗi JSON metadata mới. Debezium đẩy payload mới vào Kafka, Spark Structured Streaming ném exception `AnalysisException / SchemaMismatch` và dừng toàn bộ pipeline.
* **Phát hiện (Detection)**:
  - Alert: `spark_streaming_job_status == FAILED`.
  - Sentry / Datadog log: `DeltaSchemaEvolutionException: Schema mismatch detected on Bronze ingestion`.
* **Quy trình Khắc phục (Mitigation & Rollback)**:
  1. Router của Ingestion Engine tự động chuyển hướng các message sai schema vào **Dead-Letter Queue (DLQ)** S3 bucket `s3://vride-dead-letter/schema-anomalies/` để luồng chính tiếp tục chạy.
  2. On-call engineer kích hoạt cờ `spark.databricks.delta.schema.autoMerge.enabled = true` trên Silver pipeline.
  3. Replay lại các bản ghi trong DLQ bằng script recovery chuyên dụng mà không cần dừng hệ thống.

### Kịch bản 3: Xóa nhầm dữ liệu tài xế do lỗi phần mềm (Accidental Mass Deletion)
* **Hiện tượng lúc 3h sáng**: Một batch script bảo trì backend gửi nhầm cờ `is_deleted = true` cho toàn bộ 5.000 tài xế khu vực Hà Nội. Toàn bộ thông tin tài xế trên dashboard điều hành biến mất.
* **Phát hiện (Detection)**:
  - Alert: `deleted_records_ratio_per_batch > 5%` trong bảng `silver.driver_profiles`.
* **Quy trình Khắc phục (Mitigation & Rollback)**:
  1. Dừng ngay lập tức job sync downstream: `docker compose stop spark-streaming`.
  2. Sử dụng tính năng **Delta Time Travel** để khôi phục bảng về đúng phiên bản trước sự cố (thực hiện chỉ trong 2 giây):
     ```sql
     -- Xác định phiên bản lành lặn lúc 02:58 AM
     DESCRIBE HISTORY silver.driver_profiles;
     
     -- Khôi phục nguyên trạng
     RESTORE TABLE silver.driver_profiles TO TIMESTAMP AS OF '2026-08-18 02:58:00';
     ```
  3. Xác thực dữ liệu phục hồi và khởi động lại pipeline.

---

## 5. Ước tính Chi phí Back-of-Envelope (FinOps Breakdown)

### Giả thiết dung lượng:
- **100 triệu chuyến/năm** $\approx 274.000\text{ chuyến/ngày}$.
- Mỗi chuyến xe gồm: 1 Trip event + ~300 GPS telemetry pings (1 ping/2s trong chuyến đi 10 phút) $\approx 85\text{ triệu GPS events/ngày}$.
- Kích thước trung bình sau khi nén Parquet: ~120 bytes/event $\rightarrow$ **$\approx 10.2\text{ GB/ngày}$ raw compressed** ($\approx 3.7\text{ TB/năm}$ cho Silver; tổng dung lượng cả Bronze raw + Silver SCD2 + Gold $\approx 12\text{ TB/năm}$).

### Bảng tính chi phí hàng tháng (Cloud AWS Singapore / Local Hybrid Cloud):

| Thành phần | Công thức tính toán | Chi phí/tháng (USD) |
|---|---|:---:|
| **Storage (S3 Standard - Hot 90 ngày)** | $3\text{ TB} \times \$0.023/\text{GB-tháng}$ | \$69.00 |
| **Storage (S3 Infrequent Access - 275 ngày)** | $9\text{ TB} \times \$0.0125/\text{GB-tháng}$ | \$112.50 |
| **Storage (S3 Glacier Instant - Lưu trữ pháp lý > 1 năm)** | $12\text{ TB} \times \$0.004/\text{GB-tháng}$ | \$48.00 |
| **Streaming Compute (Spark Ingestion 24/7)** | 2 instances `c6g.xlarge` (4 vCPU, 8 GB RAM) $\times \$0.136/\text{h} \times 730\text{h}$ | \$198.56 |
| **Ad-hoc Query & Maintenance Compute** | 1 instance `r6g.xlarge` chạy on-demand / spot cho OPTIMIZE & VACUUM | \$120.00 |
| **HashiCorp Vault / KMS Tokenization API** | Cache KMS Key trong worker memory (1 call/10 phút) | \$15.00 |
| **Kafka MSK / Self-hosted Cluster** | 3 brokers `kafka.m5.large` | \$350.00 |
| **Dự phòng I/O GET/PUT & Network Egress** | ~50 triệu requests/tháng | \$85.00 |
| **TỔNG CỘNG HÀNG THÁNG** | **Toàn bộ hạ tầng Lakehouse V-Ride** | **\$998.06 / tháng** |

> 💡 **Kết luận FinOps**: Tổng chi phí chỉ xấp xỉ **\$1.000/tháng**, thấp hơn rất nhiều so với mức trần ngân sách **\$2.500/tháng**, đảm bảo tính khả thi kinh tế vượt trội cho doanh nghiệp.

---

## 6. Lộ trình Triển khai MVP 1 Tuần (Shippable MVP Slice)

Để chứng minh tính khả thi của kiến trúc với Hội đồng Kỹ thuật trong vòng **7 ngày**, nhóm sẽ xây dựng lát cắt kiến trúc nhỏ nhất (Thin Vertical Slice) bao gồm:

* **Ngày 1 - 2 (Streaming Tokenizer Spike)**:
  - Dựng pipeline Kafka $\rightarrow$ Python Structured Streaming.
  - Implement hàm `tokenize_pii()` dùng HMAC-SHA256 mã hóa số điện thoại và CCCD, ghi thử 1 triệu dòng vào Delta Bronze table trên MinIO/S3.
* **Ngày 3 - 4 (Out-of-order MERGE & SCD2 Engine)**:
  - Viết logic `MERGE` có điều kiện kiểm tra timestamp `src.event_ts > tgt.last_updated_ts`.
  - Tạo test scenario mô phỏng 10.000 event đến muộn (out-of-order 6 tiếng) và chứng minh bảng Silver không bao giờ bị thụt lùi trạng thái.
* **Ngày 5 (Decree 13 Audit & Deletion Vector Spike)**:
  - Benchmark lệnh xoá 1 khách hàng cụ thể (`DELETE WHERE phone_token = '...'`) sử dụng Deletion Vectors. Đo thời gian phản hồi ($< 300\text{ ms}$).
  - Ghi vết tự động vào bảng `compliance_audit_access_log`.
* **Ngày 6 - 7 (End-to-End Demo & Latency Benchmark)**:
  - Nối Dashboard Apache Superset / Metabase vào bảng Gold qua DuckDB/Trino.
  - Đo độ trễ từ lúc producer bắn event vào Kafka đến khi hiển thị trên biểu đồ: **Chứng minh SLA đạt < 30 giây** (Vượt cam kết 60s).
