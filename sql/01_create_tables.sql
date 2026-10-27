-- JetBrains internship exercise: proposed Meta DWH schema.
-- Target: Trino SQL with a configured Iceberg catalog named 'iceberg'.
-- Assumes the 'raw' and 'meta_dwh' schemas exist and all timestamps are UTC.
-- Logical keys are documented but NOT enforced by Iceberg.
-- This DDL is proposed, not validated against a live Trino/Iceberg cluster.

CREATE TABLE IF NOT EXISTS iceberg.meta_dwh.dim_users (
    user_id VARCHAR,
    user_name VARCHAR
);

CREATE TABLE IF NOT EXISTS iceberg.meta_dwh.dim_teams (
    team_id VARCHAR,
    team_name VARCHAR
);

-- table_id: stable hash of catalog_name.schema_name.table_name.
-- A name-based identifier does not survive a rename or table recreation.
CREATE TABLE IF NOT EXISTS iceberg.meta_dwh.dim_tables (
    table_id VARCHAR,
    catalog_name VARCHAR,
    schema_name VARCHAR,
    table_name VARCHAR
);

-- One user-team membership interval per row; [valid_from, valid_to).
-- The transformation must reject overlapping intervals for the same user.
CREATE TABLE IF NOT EXISTS iceberg.meta_dwh.user_team_history (
    user_id VARCHAR,
    team_id VARCHAR,
    valid_from TIMESTAMP(6),
    valid_to TIMESTAMP(6)
);

-- One row per query_id. Query IDs are assumed unique within one cluster.
CREATE TABLE IF NOT EXISTS iceberg.meta_dwh.fact_queries (
    query_id VARCHAR,
    user_id VARCHAR,
    started_at TIMESTAMP(6),
    completed_at TIMESTAMP(6),
    query_state VARCHAR,
    cpu_time_ms BIGINT,
    physical_input_bytes BIGINT,
    wall_time_ms BIGINT,
    peak_memory_bytes BIGINT
);

--Notice that we didn't write PRIMARY KEY (query_id).
--This doesn't mean query_id is unimportant. It means the uniqueness rule is logical, and our transformations must enforce it.


-- One row per (query_id, table_id, access_type).
CREATE TABLE IF NOT EXISTS iceberg.meta_dwh.fact_query_table_access (
    query_id VARCHAR,
    table_id VARCHAR,
    access_type VARCHAR
);

-- One row per (UTC usage_date, table_id).
-- Distinct users are NOT additive across dates.
CREATE TABLE IF NOT EXISTS iceberg.meta_dwh.agg_daily_table_usage (
    usage_date DATE,
    table_id VARCHAR,
    query_count BIGINT,
    distinct_users BIGINT
);

-- One row per (UTC usage_date, team_id).
-- max_peak_memory_bytes = maximum single-query peak, not concurrent team peak.
CREATE TABLE IF NOT EXISTS iceberg.meta_dwh.agg_daily_team_usage (
    usage_date DATE,
    team_id VARCHAR,
    query_count BIGINT,
    total_cpu_ms BIGINT,
    total_input_bytes BIGINT,
    total_wall_time_ms BIGINT,
    max_peak_memory_bytes BIGINT
);
