-- Assumption: once a query has completed, later corrections
-- remain completed records. Retractions or transitions back
-- to incomplete status are not handled by this example.
-- JetBrains Meta DWH: affected-date incremental refresh (illustrative Trino/Iceberg SQL).
-- Run 01-03 first. This file assumes dimensions, user_team_history and
-- fact_query_table_access have already been brought up to date (02/03).
-- Source: iceberg.raw.raw_query_events with ingested_at TIMESTAMP(6).
-- NOTE: This is a sequential single-run example, NOT an atomic production job.
-- It creates fixed-name staging tables; do not run concurrently. A production
-- orchestrator should use run-scoped staging, validation, failure recovery,
-- and advance its durable watermark only after successful publication.
-- Replace the example watermark bounds below with persisted batch bounds.
-- [lower, upper) prevents re-reading a boundary in the next batch.
-- A source must provide a deterministic ordering for revisions: ingested_at
-- must uniquely identify the latest event per query_id (or add version/sequence).
-- The source query_id must be globally unique for the dataset.
-- We assume changes to team history or table accesses are handled separately:
-- their affected dates must also be added to stage_affected_dates. The 03 file
-- only adds accesses; retracted accesses require reconciliation.
-- Timestamps are UTC, and the Trino session timezone must also be UTC.

-- A) Find IDs changed during the ingestion window. DISTINCT avoids duplicate IDs.
DROP TABLE IF EXISTS iceberg.meta_dwh.stage_changed_query_ids;
CREATE TABLE iceberg.meta_dwh.stage_changed_query_ids AS
SELECT DISTINCT query_id
FROM iceberg.raw.raw_query_events
WHERE ingested_at >= TIMESTAMP '2026-10-22 00:00:00'
  AND ingested_at <  TIMESTAMP '2026-10-23 00:00:00'
  AND query_id IS NOT NULL;

-- B) Select latest COMPLETE version across ALL source history for these IDs.
-- Do not select only within the watermark window: a late/stale event should
-- not replace a newer version already stored in the source.
DROP TABLE IF EXISTS iceberg.meta_dwh.stage_latest_queries;
CREATE TABLE iceberg.meta_dwh.stage_latest_queries AS
SELECT query_id, user_id, created_at AS started_at, completed_at,
       query_state, cpu_time_ms, physical_input_bytes,
       wall_time_ms, peak_memory_bytes
FROM (
    SELECT e.*,
           ROW_NUMBER() OVER (
               PARTITION BY e.query_id ORDER BY e.ingested_at DESC
           ) AS rn
    FROM iceberg.raw.raw_query_events e
    JOIN iceberg.meta_dwh.stage_changed_query_ids ids
      ON e.query_id = ids.query_id
) ranked
WHERE rn = 1 AND completed_at IS NOT NULL;

-- C) Capture BOTH old and new execution dates BEFORE updating facts.
-- This handles moving a query from October 21 to October 22.
DROP TABLE IF EXISTS iceberg.meta_dwh.stage_affected_dates;
CREATE TABLE iceberg.meta_dwh.stage_affected_dates AS
SELECT DISTINCT usage_date
FROM (
    SELECT CAST(q.started_at AS DATE) AS usage_date
    FROM iceberg.meta_dwh.fact_queries q
    JOIN iceberg.meta_dwh.stage_latest_queries s ON q.query_id = s.query_id
    UNION ALL
    SELECT CAST(started_at AS DATE) AS usage_date
    FROM iceberg.meta_dwh.stage_latest_queries
) d
WHERE usage_date IS NOT NULL;

-- D) Upsert latest query metrics. One source row per query_id is essential.
MERGE INTO iceberg.meta_dwh.fact_queries AS target
USING iceberg.meta_dwh.stage_latest_queries AS source
ON target.query_id = source.query_id
WHEN MATCHED THEN UPDATE SET
    user_id = source.user_id,
    started_at = source.started_at,
    completed_at = source.completed_at,
    query_state = source.query_state,
    cpu_time_ms = source.cpu_time_ms,
    physical_input_bytes = source.physical_input_bytes,
    wall_time_ms = source.wall_time_ms,
    peak_memory_bytes = source.peak_memory_bytes
WHEN NOT MATCHED THEN INSERT (
    query_id, user_id, started_at, completed_at, query_state,
    cpu_time_ms, physical_input_bytes, wall_time_ms, peak_memory_bytes
) VALUES (
    source.query_id, source.user_id, source.started_at, source.completed_at,
    source.query_state, source.cpu_time_ms, source.physical_input_bytes,
    source.wall_time_ms, source.peak_memory_bytes
);

-- E) Replace table-usage aggregates ONLY on affected dates.
-- These are separate DELETE/INSERT operations, so readers may temporarily
-- observe missing dates; use staged publication in production.
DELETE FROM iceberg.meta_dwh.agg_daily_table_usage
WHERE usage_date IN (
    SELECT usage_date FROM iceberg.meta_dwh.stage_affected_dates
);

INSERT INTO iceberg.meta_dwh.agg_daily_table_usage (
    usage_date, table_id, query_count, distinct_users
)
SELECT CAST(q.started_at AS DATE), a.table_id,
       COUNT(DISTINCT q.query_id), COUNT(DISTINCT q.user_id)
FROM iceberg.meta_dwh.fact_queries q
JOIN iceberg.meta_dwh.fact_query_table_access a
  ON q.query_id = a.query_id
WHERE a.access_type = 'READ'
  AND CAST(q.started_at AS DATE) IN (
      SELECT usage_date FROM iceberg.meta_dwh.stage_affected_dates
  )
GROUP BY CAST(q.started_at AS DATE), a.table_id;

-- F) Replace team-resource aggregates ONLY on affected dates.
-- No join to query-table access: CPU must not be multiplied by table count.
DELETE FROM iceberg.meta_dwh.agg_daily_team_usage
WHERE usage_date IN (
    SELECT usage_date FROM iceberg.meta_dwh.stage_affected_dates
);

INSERT INTO iceberg.meta_dwh.agg_daily_team_usage (
    usage_date, team_id, query_count, total_cpu_ms,
    total_input_bytes, total_wall_time_ms, max_peak_memory_bytes
)
SELECT CAST(q.started_at AS DATE),
       COALESCE(h.team_id, 'UNKNOWN'),
       COUNT(*), SUM(q.cpu_time_ms), SUM(q.physical_input_bytes),
       SUM(q.wall_time_ms), MAX(q.peak_memory_bytes)
FROM iceberg.meta_dwh.fact_queries q
LEFT JOIN iceberg.meta_dwh.user_team_history h
  ON q.user_id = h.user_id
 AND q.started_at >= h.valid_from
 AND (h.valid_to IS NULL OR q.started_at < h.valid_to)
WHERE CAST(q.started_at AS DATE) IN (
    SELECT usage_date FROM iceberg.meta_dwh.stage_affected_dates
)
GROUP BY CAST(q.started_at AS DATE), COALESCE(h.team_id, 'UNKNOWN');

-- G) Inspect affected dates and reconciliation before advancing watermark.
SELECT usage_date FROM iceberg.meta_dwh.stage_affected_dates
ORDER BY usage_date;

SELECT
    (SELECT SUM(cpu_time_ms) FROM iceberg.meta_dwh.fact_queries
      WHERE started_at IS NOT NULL) AS fact_cpu_ms,
    (SELECT SUM(total_cpu_ms) FROM iceberg.meta_dwh.agg_daily_team_usage)
      AS aggregated_cpu_ms;

-- The two totals must agree when each query has at most one historical team
-- match. Also check duplicate logical keys and missing/overlapping mappings.
-- Advance the persisted ingestion watermark ONLY after successful checks.
-- Do not drop staging tables until recovery/retry is no longer needed.
