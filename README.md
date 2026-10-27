# Meta DWH for Trino Query Usage Analytics

A proposed metadata data warehouse for analyzing how teams and users access tables through Trino and how much compute and I/O their queries consume. This project is a solution to the JetBrains *Data Engineering for Product Analytics* internship exercise.

> **Scope:** The source schemas and sample events are assumptions, not JetBrains production data. The local tests use DuckDB; the proposed deployment target is Trino with Apache Iceberg tables on Amazon S3. Passing DuckDB tests does not validate Trino/Iceberg compatibility.

## Questions this warehouse answers

- How many **distinct users** read a given table in an arbitrary time range?
- How much **CPU time, physical input, and query execution time** did a team consume in a period?
- How have table usage and team resource metrics changed day by day?

## Architecture

```text
Trino query events + table-access records + user/team history
                           |
                  validation / deduplication
                           |
             dimensions + query-level fact tables
                           |
                daily usage aggregate tables
                           |
                analytical SQL / dashboards
```

Raw events and the proposed warehouse are represented as Iceberg tables in a Trino catalog. The implementation examples use an assumed `iceberg.raw` source schema and `iceberg.meta_dwh` destination schema. Catalog configuration, S3 credentials, and source ingestion are outside the scope of the exercise.

## Proposed data model

| Table | Grain | Logical key / purpose |
| --- | --- | --- |
| `dim_users` | One user | `user_id`; user metadata when available |
| `dim_teams` | One team | `team_id`; team display name |
| `dim_tables` | One fully qualified table name | `table_id`; catalog, schema, and table name |
| `user_team_history` | One membership interval | User, team, `[valid_from, valid_to)` |
| `fact_queries` | One query execution | `query_id`; user, timestamps, status, resource metrics |
| `fact_query_table_access` | One query/table/access-type relationship | Query, table, access type (`READ`, etc.) |
| `agg_daily_table_usage` | One UTC date and table | Query count and distinct user count for reads |
| `agg_daily_team_usage` | One UTC date and team | Query count and resource aggregates |

`fact_queries.user_id` logically references `dim_users`; access rows reference `fact_queries` and `dim_tables`; team usage is attributed through `user_team_history`. Iceberg does not enforce traditional foreign keys, so transformations and data-quality checks must validate these relationships.

## SQL implementation

The `sql/` directory contains six scripts representing the proposed Trino/Apache Iceberg warehouse implementation.

| File | Description |
|---|---|
| [`01_create_tables.sql`](sql/01_create_tables.sql) | Defines eight warehouse tables, including dimensions, query-level facts, historical team memberships, and daily aggregates. |
| [`02_load_dimensions.sql`](sql/02_load_dimensions.sql) | Populates and updates user, team, and table dimensions, along with historical user-to-team membership intervals. Uses `MERGE` for upserts. |
| [`03_load_facts.sql`](sql/03_load_facts.sql) | Selects the latest query-event version using `ROW_NUMBER()`, upserts query-level resource metrics, and records query-to-table access relationships. |
| [`04_daily_aggregates.sql`](sql/04_daily_aggregates.sql) | Performs a full refresh of daily table usage and team resource aggregates. Counts distinct users and avoids multiplying resource metrics for multi-table queries. |
| [`05_incremental_updates.sql`](sql/05_incremental_updates.sql) | Demonstrates affected-date incremental processing. Identifies changed queries, captures old and new execution dates, updates query facts, and recalculates affected daily aggregates. |
| [`06_analytical_queries.sql`](sql/06_analytical_queries.sql) | Contains analytical SQL for distinct table users, team resource consumption, historical trends, and data-quality checks. |

### Execution order

For an initial warehouse load, run the scripts in this order:

1. `01_create_tables.sql` — create warehouse tables.
2. `02_load_dimensions.sql` — populate dimensions and team history.
3. `03_load_facts.sql` — populate query facts and table-access relationships.
4. `04_daily_aggregates.sql` — build daily aggregates.
5. `06_analytical_queries.sql` — run analytical and validation queries.

`05_incremental_updates.sql` illustrates subsequent incremental processing rather than the initial full load. It assumes relevant dimensions and query-table relationships have already been updated.

### Implementation considerations

The SQL targets a Trino catalog named `iceberg`, with source tables under `iceberg.raw` and warehouse tables under `iceberg.meta_dwh`.

The scripts assume that these schemas and source tables already exist. Source ingestion, scheduling, durable watermark management, and deployment configuration are outside the scope of this example.

The full-refresh and incremental examples use separate SQL statements, so their updates are not atomic across all warehouse tables. Production deployment would require orchestration, failure recovery, and a safe publication strategy.

The SQL is a proposed implementation and has not been executed against a live Trino/Iceberg environment.

## Metrics and attribution

- **Distinct table users:** `COUNT(DISTINCT user_id)` over query/table read accesses, with the query start timestamp within the requested period. The same user is counted once per table for the period.
- **Query count:** Distinct query executions. A query reading several tables contributes to each relevant table's usage count.
- **Team CPU:** Sum of per-query `cpu_time_ms`, attributed once to the user's team at query start. Do **not** join query/table accesses when computing team CPU, or multi-table queries may be double-counted.
- **Physical input:** Sum of per-query `physical_input_bytes`; this is data read, not an S3 billing estimate.
- **Wall time:** Sum of query durations; this is not equivalent to elapsed team activity time when queries overlap.
- **Peak memory:** Maximum *individual query* `peak_memory_bytes` in the period; it is **not** the team's simultaneous peak memory. Summing peaks would be misleading.

Failed queries are included in resource totals if they have recorded consumption. Table access records from failed queries are counted when present; this is an explicit convention that can be adjusted. Missing measurements should be tracked rather than silently treated as zero. Daily distinct-user counts **must not be summed** to calculate distinct users across a longer period; query detailed facts instead.

## Assumed source data and conventions

1. `raw_query_events`: `query_id`, `user_id`, `created_at`, `completed_at`, `query_state`, `cpu_time_ms`, `physical_input_bytes`, `wall_time_ms`, `peak_memory_bytes`, and (for incremental processing) `ingested_at`.
2. `raw_query_table_access`: `query_id`, `catalog_name`, `schema_name`, `table_name`, `access_type`.
3. `raw_user_team_history`: `user_id`, `team_id`, `team_name`, `valid_from`, `valid_to`.
4. Query IDs are unique within the analyzed cluster. With multiple clusters, add `cluster_id` to query keys.
5. Each user belongs to at most one team at any instant. Membership intervals are half-open: `valid_from <= started_at < valid_to`; `NULL valid_to` means ongoing. Missing assignments are reported under an `UNKNOWN` team in local tests.
6. Timestamps use a consistent UTC convention; daily buckets are UTC dates.
7. Multiple source events for one query may exist. The latest ingested version is authoritative in the synthetic tests; production ingestion should also define a deterministic tie-breaker.
8. A query can read multiple tables. Table-level CPU allocation is not attempted without more detailed per-source resource telemetry.
9. The `table_id` is derived from the fully qualified name for the proposed SQL; renames and table recreation need a stronger identity if continuity matters.

## Loading and historical updates

The proposed production flow is:

1. Ingest source events with an ingestion timestamp and a durable processing watermark.
2. Deduplicate incoming query versions before applying `MERGE` to query facts.
3. Upsert table-access and metadata records, validating logical keys and team-history intervals.
4. Identify affected dates from **both old and new** query timestamps when a record is corrected.
5. Recompute aggregates for affected dates from detailed facts, replacing stale rows (including keys that disappear after a correction).
6. Validate counts and resource reconciliation before advancing the watermark.

Late-arriving events are assigned to their **query execution date**, not their ingestion date. A historical team-membership correction may also require reprocessing previously aggregated dates.

The local examples demonstrate a repeatable full refresh and a controlled incremental update with `MERGE`. They do **not** implement a production scheduler, durable watermark storage, transactional orchestration across tables, or all metadata change scenarios.

## Local testing (DuckDB)

Requires Python 3 and DuckDB. From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python tests/meta_dwh_step8b.py
python tests/meta_dwh_step8c.py
```

Run Step 8B first: Step 8C starts from a copy of its database. Both scripts use synthetic records and print assertions. Expected Step 8B total CPU: **31,000 ms**. Expected Step 8C total CPU after a correction, late event, and date change: **34,000 ms**. Repeating the incremental update should not duplicate usage.


## Analytical examples

The following SQL queries demonstrate how the proposed Meta DWH answers the two main business questions. Table names, team IDs, and dates are illustrative.

### 1. How many distinct users queried table X during a given period?

Example: Count distinct users who read `hive.analytics.orders` during October 2026.

```sql
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
```

**Design note:** `COUNT(DISTINCT q.user_id)` ensures that users who queried the table multiple times are counted only once. Detailed facts are used because daily distinct-user counts cannot safely be summed across dates.

### 2. How many resources did team Y consume during that period?

Example: Calculate resource usage for team `t02` during October 2026.

```sql
SELECT
    h.team_id,
    COUNT(*) AS query_count,
    SUM(q.cpu_time_ms) / 1000.0 AS total_cpu_seconds,
    SUM(q.physical_input_bytes) / POWER(1024.0, 3) AS total_input_gib,
    SUM(q.wall_time_ms) / 1000.0 AS total_wall_seconds,
    MAX(q.peak_memory_bytes) / POWER(1024.0, 3)
        AS max_single_query_peak_memory_gib
FROM iceberg.meta_dwh.fact_queries AS q
JOIN iceberg.meta_dwh.user_team_history AS h
    ON q.user_id = h.user_id
   AND q.started_at >= h.valid_from
   AND (h.valid_to IS NULL OR q.started_at < h.valid_to)
WHERE h.team_id = 't02'
  AND q.started_at >= TIMESTAMP '2026-10-01 00:00:00'
  AND q.started_at < TIMESTAMP '2026-11-01 00:00:00'
GROUP BY h.team_id;
```

**Design note:** Team membership is resolved at query execution time using historical membership intervals. Resource metrics are aggregated directly from query facts to avoid double-counting queries that access multiple tables. Wall time represents summed query durations, and peak memory represents the maximum individual-query peak, not concurrent team memory.

### 3. How does usage change over time?

Historical daily trends are available in `agg_daily_table_usage` and `agg_daily_team_usage`.

For example:

```sql
SELECT
    usage_date,
    query_count,
    total_cpu_ms / 1000.0 AS cpu_seconds
FROM iceberg.meta_dwh.agg_daily_team_usage
WHERE team_id = 't02'
  AND usage_date >= DATE '2026-10-01'
  AND usage_date < DATE '2026-11-01'
ORDER BY usage_date;
```

The full analytical SQL examples and validation queries are available in [`sql/06_analytical_queries.sql`](sql/06_analytical_queries.sql).

**Note:** These queries illustrate the proposed design. They have not been executed against JetBrains production data.

## Limitations and further metadata

The incremental example assumes that corrections to completed queries remain completed records. Query retractions or transitions back to incomplete status are not handled. Changes to table-access relationships and historical team assignments require separate reconciliation.

- The source schema is illustrative and needs mapping to actual Trino event fields and available Iceberg metadata.
- DuckDB tests validate local transformation logic, **not** execution against Trino, Iceberg, S3, or a real metastore.
- Production use needs a robust event version/tie-breaker, durable watermark, error handling, orchestration, and recovery semantics.
- Team-history changes, deleted/retracted accesses, table renames, and cluster-specific query IDs need explicit change-management rules.
- Additional useful metadata: `cluster_id`, event version, ingestion time, query error code, query type, table snapshot ID, storage bytes, table owner, and source-specific I/O metrics.
- For large volumes, consider partitioning by date, monitoring data quality, retention policies, and appropriate Iceberg maintenance.

## Repository layout (target)

```text
jetbrains-meta-dwh/
├── README.md
├── requirements.txt
├── .gitignore
├── sql/
│   ├── 01_create_tables.sql
│   ├── 02_load_dimensions.sql
│   ├── 03_load_facts.sql
│   ├── 04_daily_aggregates.sql
│   ├── 05_incremental_updates.sql
│   └── 06_analytical_queries.sql
└── tests/
    ├── create_test_data.py
    ├── test_queries.py
    ├── meta_dwh_step8b.py
    └── meta_dwh_step8c.py
```

The repository contains a proposed Trino/Iceberg implementation and locally tested DuckDB examples. The Trino SQL has not been executed against a production cluster. The implementation is intended as a transparent exercise solution, not a claim of access to JetBrains infrastructure.
