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

The intended SQL examples cover (a) distinct users reading a fully qualified table in an arbitrary interval, (b) a team's resource usage with historical membership attribution, and (c) daily usage trends. For arbitrary-period distinct users, join detailed query and access facts rather than summing daily distinct-user aggregates.

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
