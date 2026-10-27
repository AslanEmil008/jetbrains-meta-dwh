-- 06_analytical_queries.sql
-- Target: Trino SQL over an Iceberg catalog named iceberg.
-- Assumptions: timestamps are normalized to UTC; query_id is globally unique;
-- access_type = 'READ' identifies table reads; user-team intervals do not overlap.
-- Replace example identifiers and dates for your environment.

-- 1. Distinct users who read a specified table during an arbitrary period.
-- Use detail-level facts: summing daily distinct counts would double-count users.
SELECT
    COUNT(DISTINCT q.user_id) AS distinct_users
FROM iceberg.meta_dwh.fact_queries AS q
JOIN iceberg.meta_dwh.fact_query_table_access AS a
    ON q.query_id = a.query_id
JOIN iceberg.meta_dwh.dim_tables AS t
    ON a.table_id = t.table_id
WHERE t.catalog_name = 'hive'
  AND t.schema_name = 'analytics'
  AND t.table_name = 'orders'
  AND a.access_type = 'READ'
  AND q.started_at >= TIMESTAMP '2026-10-01 00:00:00'
  AND q.started_at < TIMESTAMP '2026-11-01 00:00:00';

-- 2. Resource consumption by team over an arbitrary period.
-- Aggregate query-level facts without joining table access rows, to avoid
-- multiplying resource metrics when a query reads multiple tables.
-- Filter by stable team_id rather than team_name (which may change).
SELECT
    h.team_id,
    COUNT(*) AS query_count,
    SUM(q.cpu_time_ms) / 1000.0 AS total_cpu_seconds,
    SUM(q.physical_input_bytes) / POWER(1024.0, 3) AS total_input_gib,
    SUM(q.wall_time_ms) / 1000.0 AS sum_query_wall_seconds,
    MAX(q.peak_memory_bytes) / POWER(1024.0, 3) AS max_single_query_peak_memory_gib
FROM iceberg.meta_dwh.fact_queries AS q
JOIN iceberg.meta_dwh.user_team_history AS h
    ON q.user_id = h.user_id
   AND q.started_at >= h.valid_from
   AND (h.valid_to IS NULL OR q.started_at < h.valid_to)
WHERE h.team_id = 't02'
  AND q.started_at >= TIMESTAMP '2026-10-01 00:00:00'
  AND q.started_at < TIMESTAMP '2026-11-01 00:00:00'
GROUP BY h.team_id;
-- Missing team mappings are not represented in this specific-team query.
-- They should be reported separately as UNKNOWN in daily aggregates.

-- 3. Daily table usage trend.
SELECT
    a.usage_date,
    a.query_count,
    a.distinct_users
FROM iceberg.meta_dwh.agg_daily_table_usage AS a
JOIN iceberg.meta_dwh.dim_tables AS t
    ON a.table_id = t.table_id
WHERE t.catalog_name = 'hive'
  AND t.schema_name = 'analytics'
  AND t.table_name = 'orders'
  AND a.usage_date >= DATE '2026-10-01'
  AND a.usage_date < DATE '2026-11-01'
ORDER BY a.usage_date;

-- 4. Daily team resource trend.
SELECT
    usage_date,
    query_count,
    total_cpu_ms / 1000.0 AS cpu_seconds,
    total_input_bytes / POWER(1024.0, 3) AS input_gib,
    max_peak_memory_bytes / POWER(1024.0, 3) AS max_single_query_peak_memory_gib
FROM iceberg.meta_dwh.agg_daily_team_usage
WHERE team_id = 't02'
  AND usage_date >= DATE '2026-10-01'
  AND usage_date < DATE '2026-11-01'
ORDER BY usage_date;

-- 5. Validation: query IDs must be unique.
-- Expected result: zero rows.
SELECT query_id, COUNT(*) AS row_count
FROM iceberg.meta_dwh.fact_queries
GROUP BY query_id
HAVING COUNT(*) > 1;

-- 6. Validation: duplicate query/table/access relationships.
-- Expected result: zero rows.
SELECT query_id, table_id, access_type, COUNT(*) AS row_count
FROM iceberg.meta_dwh.fact_query_table_access
GROUP BY query_id, table_id, access_type
HAVING COUNT(*) > 1;

-- 7. Validation: query executions matching multiple team-history rows.
-- Expected result: zero rows if team assignment is single-valued.
SELECT q.query_id, COUNT(*) AS matching_memberships
FROM iceberg.meta_dwh.fact_queries AS q
JOIN iceberg.meta_dwh.user_team_history AS h
    ON q.user_id = h.user_id
   AND q.started_at >= h.valid_from
   AND (h.valid_to IS NULL OR q.started_at < h.valid_to)
GROUP BY q.query_id
HAVING COUNT(*) > 1;

-- 8. Validation: queries without a historical team assignment.
SELECT COUNT(*) AS queries_without_team
FROM iceberg.meta_dwh.fact_queries AS q
WHERE NOT EXISTS (
    SELECT 1
    FROM iceberg.meta_dwh.user_team_history AS h
    WHERE q.user_id = h.user_id
      AND q.started_at >= h.valid_from
      AND (h.valid_to IS NULL OR q.started_at < h.valid_to)
);
