-- JetBrains Meta DWH: load query facts and query-table relationships.
-- Dialect: proposed Trino SQL for an Iceberg catalog. Not cluster-tested.
-- Run AFTER 01_create_tables.sql and 02_load_dimensions.sql.
--
-- Additional explicit source assumption for reliable corrections:
-- iceberg.raw.raw_query_events contains ingested_at TIMESTAMP(6), an
-- ingestion timestamp that increases when a newer version of a query arrives.
-- For each query_id, the newest ingested_at must be unique; ties must be
-- resolved upstream using an event version/sequence. Validate this before MERGE.
-- All event timestamps are UTC; query_id is globally unique in this dataset.
-- The source also contains: created_at, completed_at, query_state,
-- cpu_time_ms, physical_input_bytes, wall_time_ms, peak_memory_bytes.
--
-- This demonstration reads the entire raw source and upserts latest records.
-- A production incremental pipeline should stage changed query IDs using an
-- ingestion watermark, then reprocess the affected historical dates.

-- 1) QUERY FACTS: keep the latest version per query_id,
--    provided that its completed_at is not NULL.
MERGE INTO iceberg.meta_dwh.fact_queries AS target
USING (
    SELECT
        query_id,
        user_id,
        created_at AS started_at,
        completed_at,
        query_state,
        cpu_time_ms,
        physical_input_bytes,
        wall_time_ms,
        peak_memory_bytes
    FROM (
        SELECT
            e.*,
            ROW_NUMBER() OVER (
                PARTITION BY query_id
                ORDER BY ingested_at DESC
            ) AS rn
        FROM iceberg.raw.raw_query_events AS e
        WHERE query_id IS NOT NULL
    ) AS ranked
    WHERE rn = 1
      AND completed_at IS NOT NULL
) AS source
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
    query_id,
    user_id,
    started_at,
    completed_at,
    query_state,
    cpu_time_ms,
    physical_input_bytes,
    wall_time_ms,
    peak_memory_bytes
) VALUES (
    source.query_id,
    source.user_id,
    source.started_at,
    source.completed_at,
    source.query_state,
    source.cpu_time_ms,
    source.physical_input_bytes,
    source.wall_time_ms,
    source.peak_memory_bytes
);

-- 2) QUERY-TABLE ACCESSES: one row per (query_id, table_id, access_type).
-- Only include query IDs present in fact_queries. A query may have multiple
-- table accesses, but its CPU/memory/input metrics remain in fact_queries.
-- This assumes raw access records are an additive history (no retractions).
-- If accesses can be corrected/deleted, reconcile all accesses for the
-- changed query IDs by replacing their old relationship rows.
MERGE INTO iceberg.meta_dwh.fact_query_table_access AS target
USING (
    SELECT DISTINCT
        a.query_id,
        t.table_id,
        a.access_type
    FROM iceberg.raw.raw_query_table_access AS a
    JOIN iceberg.meta_dwh.dim_tables AS t
      ON a.catalog_name = t.catalog_name
     AND a.schema_name = t.schema_name
     AND a.table_name = t.table_name
    JOIN iceberg.meta_dwh.fact_queries AS q
      ON a.query_id = q.query_id
    WHERE a.query_id IS NOT NULL
      AND a.access_type IS NOT NULL
) AS source
ON target.query_id = source.query_id
   AND target.table_id = source.table_id
   AND target.access_type = source.access_type
WHEN NOT MATCHED THEN INSERT (
    query_id, table_id, access_type
) VALUES (
    source.query_id, source.table_id, source.access_type
);

-- Suggested validation checks (expected result: zero rows):
-- SELECT query_id, COUNT(*) AS n FROM iceberg.meta_dwh.fact_queries
-- GROUP BY query_id HAVING COUNT(*) > 1;
-- SELECT query_id, table_id, access_type, COUNT(*) AS n
-- FROM iceberg.meta_dwh.fact_query_table_access
-- GROUP BY query_id, table_id, access_type HAVING COUNT(*) > 1;
-- SELECT query_id, ingested_at, COUNT(*) AS n
-- FROM iceberg.raw.raw_query_events
-- GROUP BY query_id, ingested_at HAVING COUNT(*) > 1;
-- The last check is conservative: exact repeated raw events may be harmless,
-- but ambiguous same-timestamp corrections require a deterministic tie-breaker.
