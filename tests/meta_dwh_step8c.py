"""Incremental-update exercise for the DuckDB warehouse created in Step 8B.
Runs on an isolated copy of the database. Requires: pip install duckdb
"""
from pathlib import Path
import shutil
import duckdb

folder = Path(__file__).resolve().parent
source_db = folder / 'meta_dwh_step8b.duckdb'
test_db = folder / 'meta_dwh_step8c.duckdb'
if not source_db.exists():
    raise SystemExit('Run meta_dwh_step8b.py first to create meta_dwh_step8b.duckdb')
shutil.copy2(source_db, test_db)
con = duckdb.connect(str(test_db))

# One corrected query, one late query, and one changed execution date.
con.execute("""
INSERT INTO raw_query_events VALUES
 ('q102','u02',TIMESTAMP '2026-10-20 11:00:00',TIMESTAMP '2026-10-20 11:01:00','FINISHED',9500,2097152,60000,3145728,TIMESTAMP '2026-10-24 09:00:00'),
 ('q108','u03',TIMESTAMP '2026-10-20 15:00:00',TIMESTAMP '2026-10-20 15:01:00','FINISHED',2500,1048576,60000,2097152,TIMESTAMP '2026-10-24 09:01:00'),
 ('q103','u01',TIMESTAMP '2026-10-22 12:00:00',TIMESTAMP '2026-10-22 12:01:00','FINISHED',3000,524288,60000,1048576,TIMESTAMP '2026-10-24 09:02:00')
""")
con.execute("INSERT INTO raw_query_table_access VALUES ('q108','hive','analytics','orders','READ')")

# Simulated ingestion watermark. Only changes after 2026-10-22 12:00 are new.
watermark = "2026-10-22 12:00:00"

def run_incremental():
    # Stage one latest version per query ID, not multiple matches for MERGE.
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE incoming AS
    SELECT query_id,user_id,created_at AS started_at,completed_at,query_state,
           cpu_time_ms,physical_input_bytes,wall_time_ms,peak_memory_bytes
    FROM raw_query_events
    WHERE ingested_at > TIMESTAMP '{watermark}' AND completed_at IS NOT NULL
    QUALIFY ROW_NUMBER() OVER
       (PARTITION BY query_id ORDER BY ingested_at DESC, completed_at DESC)=1
    """)
    # Capture BOTH old and new dates: a corrected timestamp can move a query.
    con.execute("""
    CREATE OR REPLACE TEMP TABLE affected_dates AS
    SELECT DISTINCT CAST(q.started_at AS DATE) AS usage_date
      FROM fact_queries q JOIN incoming i USING(query_id)
    UNION
    SELECT DISTINCT CAST(started_at AS DATE) FROM incoming
    """)
    con.execute('BEGIN TRANSACTION')
    try:
        con.execute("""
        MERGE INTO fact_queries AS target USING incoming AS source
        ON target.query_id=source.query_id
        WHEN MATCHED THEN UPDATE SET
          user_id=source.user_id,started_at=source.started_at,
          completed_at=source.completed_at,query_state=source.query_state,
          cpu_time_ms=source.cpu_time_ms,
          physical_input_bytes=source.physical_input_bytes,
          wall_time_ms=source.wall_time_ms,
          peak_memory_bytes=source.peak_memory_bytes
        WHEN NOT MATCHED THEN INSERT
          (query_id,user_id,started_at,completed_at,query_state,cpu_time_ms,
           physical_input_bytes,wall_time_ms,peak_memory_bytes)
        VALUES (source.query_id,source.user_id,source.started_at,
                source.completed_at,source.query_state,source.cpu_time_ms,
                source.physical_input_bytes,source.wall_time_ms,source.peak_memory_bytes)
        """)
        # Rebuild only access rows belonging to changed queries. Handles removed accesses too.
        con.execute("DELETE FROM fact_query_table_access WHERE query_id IN (SELECT query_id FROM incoming)")
        con.execute("""
        INSERT INTO fact_query_table_access
        SELECT DISTINCT a.query_id,t.table_id,a.access_type
        FROM raw_query_table_access a
        JOIN dim_tables t USING(catalog_name,schema_name,table_name)
        JOIN incoming i ON i.query_id=a.query_id
        """)
        # Delete and recompute the affected daily partitions, including obsolete keys.
        con.execute("DELETE FROM agg_daily_table_usage WHERE usage_date IN (SELECT usage_date FROM affected_dates)")
        con.execute("""
        INSERT INTO agg_daily_table_usage
        SELECT CAST(q.started_at AS DATE),a.table_id,
               COUNT(DISTINCT q.query_id),COUNT(DISTINCT q.user_id)
        FROM fact_queries q JOIN fact_query_table_access a USING(query_id)
        WHERE a.access_type='READ' AND CAST(q.started_at AS DATE) IN (SELECT usage_date FROM affected_dates)
        GROUP BY 1,2
        """)
        con.execute("DELETE FROM agg_daily_team_usage WHERE usage_date IN (SELECT usage_date FROM affected_dates)")
        con.execute("""
        INSERT INTO agg_daily_team_usage
        SELECT CAST(q.started_at AS DATE),COALESCE(h.team_id,'UNKNOWN'),
               COUNT(*),SUM(q.cpu_time_ms),SUM(q.physical_input_bytes),
               SUM(q.wall_time_ms),MAX(q.peak_memory_bytes)
        FROM fact_queries q LEFT JOIN user_team_history h
          ON q.user_id=h.user_id AND q.started_at>=h.valid_from
          AND (h.valid_to IS NULL OR q.started_at<h.valid_to)
        WHERE CAST(q.started_at AS DATE) IN (SELECT usage_date FROM affected_dates)
        GROUP BY 1,2
        """)
        con.execute('COMMIT')
    except Exception:
        con.execute('ROLLBACK')
        raise

run_incremental()
assert con.execute("SELECT COUNT(*) FROM fact_queries").fetchone()[0] == 8
assert con.execute("SELECT cpu_time_ms FROM fact_queries WHERE query_id='q102'").fetchone()[0] == 9500
assert con.execute("SELECT CAST(started_at AS DATE) FROM fact_queries WHERE query_id='q103'").fetchone()[0].isoformat() == '2026-10-22'
assert con.execute("SELECT total_cpu_ms FROM agg_daily_team_usage WHERE usage_date=DATE '2026-10-20' AND team_id='team_A'").fetchone()[0] == 7500
assert con.execute("SELECT total_cpu_ms FROM agg_daily_team_usage WHERE usage_date=DATE '2026-10-20' AND team_id='team_B'").fetchone()[0] == 11500
assert con.execute("SELECT COUNT(*) FROM agg_daily_team_usage WHERE usage_date=DATE '2026-10-21' AND team_id='team_A'").fetchone()[0] == 1
assert con.execute("SELECT SUM(total_cpu_ms) FROM agg_daily_team_usage").fetchone()[0] == 34000
before = con.execute('SELECT COUNT(*),SUM(total_cpu_ms) FROM agg_daily_team_usage').fetchone()
run_incremental()
after = con.execute('SELECT COUNT(*),SUM(total_cpu_ms) FROM agg_daily_team_usage').fetchone()
assert before == after
assert con.execute("SELECT COUNT(*) FROM fact_queries").fetchone()[0] == 8
print('PASS: corrected q102 CPU = 9,500 ms')
print('PASS: late q108 counted on October 20')
print('PASS: moved q103 from October 21 to October 22')
print('PASS: affected aggregates recomputed; total CPU = 34,000 ms')
print('PASS: repeated incremental run did not duplicate records')
print(con.execute('SELECT usage_date,team_id,query_count,total_cpu_ms FROM agg_daily_team_usage ORDER BY 1,2').fetchdf().to_string(index=False))
con.close()
