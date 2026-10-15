# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Thành Phong / 2A202602843
**Repo:** https://github.com/kaisye/K4-Track02-Day17-NguyenThanhPhong-2A202602843-DataPipelineEngineering
**Commit bài nộp:**
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code — hỗ trợ định vị và sửa 3 lỗi, chạy kiểm tra; tôi đã đọc và hiểu từng thay đổi.
**Nguồn tham khảo khác (nếu có):**

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets` có nhiều hàng/ticket; T-91 không về trạng thái mới nhất; rerun lệch checksum | `gold_feature_daily` không khớp full recompute; event của u05 (12/08, đến 15/08) không vào ngày 12/08 | T-97 vẫn còn trong Silver, training set, RAG index |
| **Nguyên nhân gốc** | `upsert_silver_tickets` dùng `INSERT` thuần — không có upsert theo khoá, chạy lại là nhân bản hàng | `LOOKBACK_DAYS = 0` (đoán "event đến trong vài giây") trong khi P99 đo từ Bronze = 3 ngày | Bản ghi `op='d'` có `after = null`, nên `ticket_id` lấy từ `after` là NULL và bị lọc mất; delete không bao giờ tới Silver |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: `MERGE INTO silver_tickets ON ticket_id`, `WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE`, `NOT MATCHED THEN INSERT` | `pipeline/config.py`: `LOOKBACK_DAYS = 3` | `pipeline/staging.py`: `coalesce(key.ticket_id, after.ticket_id, before.ticket_id)` — delete thành tombstone `is_deleted` với PII null, lan xuống Gold |
| **Khái niệm trên slide** | Silver — có khoá; 4 cách viết idempotent (MERGE) | Data về muộn; lookback = P99 đo từ Bronze | CDC log-based (Debezium before/after/op); Xoá phải lan |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum (combined): `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY (silver_tickets `3c15dfd43701`, gold_feature_daily `8630e04a61d1`)

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: ticket là thực thể có trạng thái nên cần upsert theo khoá + LSN để chạy lại/replay ngày cũ không đổi kết quả; Gold là aggregate theo ngày nên xoá-rồi-ghi cả partition (cả cửa sổ lookback) luôn cho cùng kết quả.
- Tombstone thay vì xoá hẳn hàng trong Silver: giữ dấu vết xoá (replay batch cũ không làm ticket 'sống lại', nhờ so LSN) trong khi vẫn xoá sạch PII.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: tái lập được và kiểm toán được; mẫu training luôn truy ngược về đúng trạng thái dữ liệu lúc đó, sửa sai thì tạo version mới.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: dữ liệu chỉ vài chục dòng, DuckDB chạy in-process, zero-key, dễ tái lập; dbt cho SQL có contract/test/microbatch khai báo. Spark chỉ đáng khi dữ liệu vượt một máy.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?

   **Trả lời:** Quyền xoá thắng về nội dung, bất biến vẫn giữ về cấu trúc: thay vì sửa snapshot, ta xoá/che dòng của T-97 ở mọi version cũ (hoặc huỷ version và phát hành lại version mới bump có ghi chú), ghi vào audit log rằng đã redact theo yêu cầu xoá, và đặt vòng đời (retention) cho snapshot. Bronze giữ raw cũng phải có TTL/xoá theo khoá. Model đã train trên snapshot cũ cần được đánh dấu để huấn luyện lại ở chu kỳ tới.
2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?

   **Trả lời:** Đặt chốt ở ranh giới Bronze→Silver (trước mọi tầng dùng chung), kết hợp regex với NER (nhận diện tên người, tiếng Việt có dấu) hoặc LLM/Presidio; PII còn lại ở Bronze phải mã hoá + hạn chế truy cập. Đo bằng tập golden có nhãn thủ công: recall PII (ưu tiên) và precision, cộng quét định kỳ Silver/Gold bằng detector độc lập và cảnh báo khi phát hiện PII sót.

## 5. Output (dán nguyên văn)

```text
$ make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ make test
..................................                                       [100%]
34 passed in 1.02s

$ make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ make dbt
cd dbt_project && DBT_PROFILES_DIR=. /Users/phongnguyen/Desktop/AI20K/LAB/DAY17/K4-Track02-Day17-NguyenThanhPhong-2A202602843-DataPipelineEngineering/.venv/bin/dbt build --event-time-start 2026-08-10 --event-time-end 2026-08-17
18:27:49  Running with dbt=1.12.5
18:27:49  Registered adapter: duckdb=1.11.0
18:27:49  Unable to do partial parsing because saved manifest not found. Starting full parse.
18:27:50  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
18:27:50  
18:27:50  Concurrency: 1 threads (target='dev')
18:27:50  
18:27:50  1 of 19 START sql view model main.stg_events ................................... [RUN]
18:27:50  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.04s]
18:27:50  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
18:27:50  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.01s]
18:27:50  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
18:27:51  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.05s]
18:27:51  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
18:27:51  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.11s]
18:27:51  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
18:27:51  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.05s]
18:27:51  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
18:27:51  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.02s]
18:27:51  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
18:27:51  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.01s]
18:27:51  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
18:27:51  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.01s]
18:27:51  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
18:27:51  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.04s]
18:27:51  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
18:27:51  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.04s]
18:27:51  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
18:27:51  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.01s]
18:27:51  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
18:27:51  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.01s]
18:27:51  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
18:27:51  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.01s]
18:27:51  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
18:27:51  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.01s]
18:27:51  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
18:27:51  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.01s]
18:27:51  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
18:27:51  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
18:27:51  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.01s]
18:27:51  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
18:27:51  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.03s]
18:27:51  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
18:27:51  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.02s]
18:27:51  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
18:27:51  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.01s]
18:27:51  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
18:27:51  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.01s]
18:27:51  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
18:27:51  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.01s]
18:27:51  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
18:27:51  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.01s]
18:27:51  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.13s]
18:27:51  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
18:27:51  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.01s]
18:27:51  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
18:27:51  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.01s]
18:27:51  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
18:27:51  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.01s]
18:27:51  
18:27:51  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 0.70 seconds (0.70s).
18:27:51  
18:27:51  Completed successfully
18:27:51  
18:27:51  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

Nếu dùng PowerShell, ghi lệnh tương đương và output thực tế theo [SUBMISSION.md](../docs/SUBMISSION.md).
Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.
