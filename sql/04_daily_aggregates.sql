-- JetBrains Meta DWH: rebuild daily usage aggregates from detailed facts.
-- Dialect: proposed Trino SQL for Iceberg; not tested on a live cluster.
-- Run AFTER 01_create_tables.sql, 02_load_dimensions.sql, 03_load_facts.sql.
--
-- This file is a REPEATABLE FULL REFRESH, not an incremental update.
-- DELETE + INSERT are separate statements, NOT one atomic transaction.
-- During a refresh, readers could observe empty or partially loaded data;
-- failures require rerunning the refresh. In production, use staged tables
-- and an atomic publication/swap strategy supported by the environment.
-- See 05_incremental_updates.sql for affected-date processing.
--
-- Assumptions:
-- * All timestamps are stored consistently in UTC; CAST(... AS DATE) is UTC.
-- * fact_queries contains exactly one row per query_id.
-- * fact_query_table_access contains unique (query_id, table_id, access_type).
-- * Each user matches AT MOST one team-history interval at query start.
-- * No match is attributed to the explicit 'UNKNOWN' team_id bucket.
--   This is a sentinel, not an enforced reference to dim_teams.
-- * NULL metric values are not converted to zero. SUM ignores NULL;
--   if all measurements in a group are NULL, SUM returns NULL.
-- * Only READ accesses contribute to table-usage aggregates.

-- 1) Daily table usage: one row per (usage_date, table_id).
-- COUNT(DISTINCT ...) guards against a query having multiple access types
-- or repeated source records. Do NOT sum distinct_users across dates.
DELETE FROM iceberg.meta_dwh.agg_daily_table_usage;

INSERT INTO iceberg.meta_dwh.agg_daily_table_usage (
    usage_date,
    table_id,
    query_count,
    distinct_users
)
SELECT
    CAST(q.started_at AS DATE) AS usage_date,
    a.table_id,
    COUNT(DISTINCT q.query_id) AS query_count,
    COUNT(DISTINCT q.user_id) AS distinct_users
FROM iceberg.meta_dwh.fact_queries AS q
JOIN iceberg.meta_dwh.fact_query_table_access AS a
  ON q.query_id = a.query_id
WHERE a.access_type = 'READ'
  AND q.started_at IS NOT NULL
GROUP BY CAST(q.started_at AS DATE), a.table_id;

-- 2) Daily team resources: one row per (usage_date, team_id).
-- Crucially, DO NOT JOIN fact_query_table_access here: that would multiply
-- resource usage for queries that read several tables.
DELETE FROM iceberg.meta_dwh.agg_daily_team_usage;

INSERT INTO iceberg.meta_dwh.agg_daily_team_usage (
    usage_date,
    team_id,
    query_count,
    total_cpu_ms,
    total_input_bytes,
    total_wall_time_ms,
    max_peak_memory_bytes
)
SELECT
    CAST(q.started_at AS DATE) AS usage_date,
    COALESCE(h.team_id, 'UNKNOWN') AS team_id,
    COUNT(*) AS query_count,
    SUM(q.cpu_time_ms) AS total_cpu_ms,
    SUM(q.physical_input_bytes) AS total_input_bytes,
    SUM(q.wall_time_ms) AS total_wall_time_ms,
    MAX(q.peak_memory_bytes) AS max_peak_memory_bytes
FROM iceberg.meta_dwh.fact_queries AS q
LEFT JOIN iceberg.meta_dwh.user_team_history AS h
  ON q.user_id = h.user_id
 AND q.started_at >= h.valid_from
 AND (h.valid_to IS NULL OR q.started_at < h.valid_to)
WHERE q.started_at IS NOT NULL
GROUP BY CAST(q.started_at AS DATE), COALESCE(h.team_id, 'UNKNOWN');

-- VALIDATION 1: both aggregates must have unique logical keys.
-- Expected: zero rows from each query.
SELECT usage_date, table_id, COUNT(*) AS duplicates
FROM iceberg.meta_dwh.agg_daily_table_usage
GROUP BY usage_date, table_id
HAVING COUNT(*) > 1;

SELECT usage_date, team_id, COUNT(*) AS duplicates
FROM iceberg.meta_dwh.agg_daily_team_usage
GROUP BY usage_date, team_id
HAVING COUNT(*) > 1;

-- VALIDATION 2: each query should match no more than one historical team.
-- Expected: zero rows. If this fails, team CPU can be double-counted.
SELECT q.query_id, COUNT(*) AS matched_memberships
FROM iceberg.meta_dwh.fact_queries AS q
JOIN iceberg.meta_dwh.user_team_history AS h
  ON q.user_id = h.user_id
 AND q.started_at >= h.valid_from
 AND (h.valid_to IS NULL OR q.started_at < h.valid_to)
GROUP BY q.query_id
HAVING COUNT(*) > 1;

-- VALIDATION 3: total query counts should reconcile (when all queries
-- have started_at). CPU SUM can be NULL if all CPU metrics are missing.
SELECT
    (SELECT COUNT(*) FROM iceberg.meta_dwh.fact_queries
      WHERE started_at IS NOT NULL) AS fact_query_count,
    (SELECT SUM(query_count) FROM iceberg.meta_dwh.agg_daily_team_usage)
      AS aggregated_query_count,
    (SELECT SUM(cpu_time_ms) FROM iceberg.meta_dwh.fact_queries
      WHERE started_at IS NOT NULL) AS fact_cpu_ms,
    (SELECT SUM(total_cpu_ms) FROM iceberg.meta_dwh.agg_daily_team_usage)
      AS aggregated_cpu_ms;
