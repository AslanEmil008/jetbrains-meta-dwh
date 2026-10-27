-- JetBrains Meta DWH: initial/incremental dimension upserts (Trino + Iceberg).
-- Run after 01_create_tables.sql and before loading query/table-access facts.
-- ASSUMED SOURCES:
--   iceberg.raw.raw_query_events(query_id, user_id, created_at, ...)
--   iceberg.raw.raw_user_team_history(user_id, team_id, team_name, valid_from, valid_to)
--   iceberg.raw.raw_query_table_access(query_id, catalog_name, schema_name,
--                                      table_name, access_type)
-- Assumes UTC timestamps, stable team names as of their latest valid_from,
-- and nonoverlapping membership intervals per user.
-- Source schema/semantics are hypothetical, not confirmed by JetBrains.
-- MERGE makes repeated loads safe for existing logical keys if the target is
-- already unique. It does NOT repair pre-existing duplicates or handle deletes.

-- 1) USERS: source contains user IDs, not names. Do not invent names.
MERGE INTO iceberg.meta_dwh.dim_users AS target
USING (
    SELECT DISTINCT user_id
    FROM (
        SELECT user_id FROM iceberg.raw.raw_query_events
        UNION ALL
        SELECT user_id FROM iceberg.raw.raw_user_team_history
    ) AS users
    WHERE user_id IS NOT NULL
) AS source
ON target.user_id = source.user_id
WHEN NOT MATCHED THEN
    INSERT (user_id, user_name)
    VALUES (source.user_id, CAST(NULL AS VARCHAR));

-- 2) TEAMS: latest known name for each team_id.
-- If a team is renamed, this dimension represents its current name, not
-- historical names; historical attribution uses stable team_id.
MERGE INTO iceberg.meta_dwh.dim_teams AS target
USING (
    SELECT
        team_id,
        max_by(team_name, valid_from) AS team_name
    FROM iceberg.raw.raw_user_team_history
    WHERE team_id IS NOT NULL AND valid_from IS NOT NULL
    GROUP BY team_id
) AS source
ON target.team_id = source.team_id
WHEN MATCHED THEN UPDATE SET
    team_name = source.team_name
WHEN NOT MATCHED THEN
    INSERT (team_id, team_name)
    VALUES (source.team_id, source.team_name);

-- 3) TABLES: stable ID for a normalized fully qualified name.
-- This assumes names are consistently normalized and do not contain
-- ambiguous separators. A native immutable table identifier is preferable
-- if source metadata supplies one.
MERGE INTO iceberg.meta_dwh.dim_tables AS target
USING (
    SELECT DISTINCT
        to_hex(md5(to_utf8(
            catalog_name || '.' || schema_name || '.' || table_name
        ))) AS table_id,
        catalog_name,
        schema_name,
        table_name
    FROM iceberg.raw.raw_query_table_access
    WHERE catalog_name IS NOT NULL
      AND schema_name IS NOT NULL
      AND table_name IS NOT NULL
) AS source
ON target.table_id = source.table_id
WHEN MATCHED THEN UPDATE SET
    catalog_name = source.catalog_name,
    schema_name = source.schema_name,
    table_name = source.table_name
WHEN NOT MATCHED THEN
    INSERT (table_id, catalog_name, schema_name, table_name)
    VALUES (source.table_id, source.catalog_name,
            source.schema_name, source.table_name);

-- 4) TEAM HISTORY: upsert each membership interval by (user_id, valid_from).
-- Assumes one membership per user at a given instant. If multiple teams
-- can apply concurrently, the model and attribution policy must change.
-- If a source interval is removed, MERGE alone will not delete the old row;
-- a full history reconciliation or explicit delete step is then required.
MERGE INTO iceberg.meta_dwh.user_team_history AS target
USING (
    SELECT DISTINCT user_id, team_id, valid_from, valid_to
    FROM iceberg.raw.raw_user_team_history
    WHERE user_id IS NOT NULL
      AND team_id IS NOT NULL
      AND valid_from IS NOT NULL
      AND (valid_to IS NULL OR valid_to > valid_from)
) AS source
ON target.user_id = source.user_id
   AND target.valid_from = source.valid_from
WHEN MATCHED THEN UPDATE SET
    team_id = source.team_id,
    valid_to = source.valid_to
WHEN NOT MATCHED THEN
    INSERT (user_id, team_id, valid_from, valid_to)
    VALUES (source.user_id, source.team_id,
            source.valid_from, source.valid_to);

-- VALIDATION TO RUN AFTER THE LOAD:
-- 1) Each dimension's logical key must be unique.
-- 2) No user has overlapping team membership intervals.
-- 3) Source records with identical (user_id, valid_from) must agree;
--    SELECT DISTINCT alone does not resolve conflicting versions.
-- 4) Missing user/team IDs should be monitored, not silently ignored.
