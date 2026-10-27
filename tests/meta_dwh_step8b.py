import duckdb
from pathlib import Path

DB = Path(__file__).with_name('meta_dwh_step8b.duckdb')
con = duckdb.connect(str(DB))

# Synthetic source: multiple versions of q102, late q106, one unknown team, multi-table q101.
con.execute("""
CREATE OR REPLACE TABLE raw_query_events AS
SELECT * FROM (VALUES
 ('q101','u01',TIMESTAMP '2026-10-20 10:00:00',TIMESTAMP '2026-10-20 10:01:00','FINISHED',5000::BIGINT,1048576::BIGINT,60000::BIGINT,2097152::BIGINT,TIMESTAMP '2026-10-20 10:02:00'),
 ('q102','u02',TIMESTAMP '2026-10-20 11:00:00',TIMESTAMP '2026-10-20 11:01:00','FINISHED',8000::BIGINT,2097152::BIGINT,60000::BIGINT,3145728::BIGINT,TIMESTAMP '2026-10-20 11:02:00'),
 ('q102','u02',TIMESTAMP '2026-10-20 11:00:00',TIMESTAMP '2026-10-20 11:01:00','FINISHED',9000::BIGINT,2097152::BIGINT,60000::BIGINT,3145728::BIGINT,TIMESTAMP '2026-10-20 11:03:00'),
 ('q103','u01',TIMESTAMP '2026-10-21 12:00:00',TIMESTAMP '2026-10-21 12:01:00','FINISHED',3000::BIGINT,524288::BIGINT,60000::BIGINT,1048576::BIGINT,TIMESTAMP '2026-10-21 12:02:00'),
 ('q104','u03',TIMESTAMP '2026-10-21 13:00:00',TIMESTAMP '2026-10-21 13:01:00','FAILED',7000::BIGINT,1048576::BIGINT,60000::BIGINT,2097152::BIGINT,TIMESTAMP '2026-10-21 13:02:00'),
 ('q105','u01',TIMESTAMP '2026-11-02 09:00:00',TIMESTAMP '2026-11-02 09:01:00','FINISHED',4000::BIGINT,1048576::BIGINT,60000::BIGINT,2097152::BIGINT,TIMESTAMP '2026-11-02 09:02:00'),
 ('q106','u02',TIMESTAMP '2026-10-20 14:00:00',TIMESTAMP '2026-10-20 14:01:00','FINISHED',2000::BIGINT,524288::BIGINT,60000::BIGINT,1048576::BIGINT,TIMESTAMP '2026-10-22 09:00:00'),
 ('q107','u04',TIMESTAMP '2026-10-21 14:00:00',TIMESTAMP '2026-10-21 14:01:00','FINISHED',1000::BIGINT,262144::BIGINT,60000::BIGINT,524288::BIGINT,TIMESTAMP '2026-10-21 14:02:00')
) t(query_id,user_id,created_at,completed_at,query_state,cpu_time_ms,physical_input_bytes,wall_time_ms,peak_memory_bytes,ingested_at)
""")
con.execute("""
CREATE OR REPLACE TABLE raw_query_table_access AS
SELECT * FROM (VALUES
 ('q101','hive','analytics','orders','READ'),
 ('q101','hive','analytics','customers','READ'),
 ('q101','hive','analytics','orders','READ'),
 ('q102','hive','analytics','orders','READ'),
 ('q103','hive','analytics','orders','READ'),
 ('q104','hive','analytics','payments','READ'),
 ('q105','hive','analytics','orders','READ'),
 ('q106','hive','analytics','orders','READ'),
 ('q107','hive','analytics','orders','READ')
) t(query_id,catalog_name,schema_name,table_name,access_type)
""")
con.execute("""
CREATE OR REPLACE TABLE raw_user_team_history AS
SELECT * FROM (VALUES
 ('u01','team_A','Team A',TIMESTAMP '2026-10-01',TIMESTAMP '2026-11-01'),
 ('u01','team_B','Team B',TIMESTAMP '2026-11-01',NULL),
 ('u02','team_B','Team B',TIMESTAMP '2026-10-01',NULL),
 ('u03','team_A','Team A',TIMESTAMP '2026-10-01',NULL)
) t(user_id,team_id,team_name,valid_from,valid_to)
""")

def refresh():
    # For local testing: idempotent full refresh. Production incremental MERGE is separate.
    con.execute("""
    CREATE OR REPLACE TABLE fact_queries AS
    SELECT query_id,user_id,created_at AS started_at,completed_at,query_state,
           cpu_time_ms,physical_input_bytes,wall_time_ms,peak_memory_bytes
    FROM raw_query_events
    WHERE query_id IS NOT NULL AND completed_at IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY query_id ORDER BY ingested_at DESC, completed_at DESC) = 1
    """)
    con.execute("""
    CREATE OR REPLACE TABLE dim_users AS
    SELECT DISTINCT user_id, NULL::VARCHAR AS user_name FROM fact_queries WHERE user_id IS NOT NULL
    """)
    con.execute("""
    CREATE OR REPLACE TABLE dim_teams AS
    SELECT team_id, MAX(team_name) AS team_name FROM raw_user_team_history
    WHERE team_id IS NOT NULL GROUP BY team_id
    UNION ALL SELECT 'UNKNOWN', 'Unknown team'
    """)
    con.execute("""
    CREATE OR REPLACE TABLE user_team_history AS
    SELECT DISTINCT user_id,team_id,valid_from,valid_to FROM raw_user_team_history
    """)
    con.execute("""
    CREATE OR REPLACE TABLE dim_tables AS
    SELECT DISTINCT md5(catalog_name || '.' || schema_name || '.' || table_name) AS table_id,
           catalog_name,schema_name,table_name
    FROM raw_query_table_access
    WHERE catalog_name IS NOT NULL AND schema_name IS NOT NULL AND table_name IS NOT NULL
    """)
    con.execute("""
    CREATE OR REPLACE TABLE fact_query_table_access AS
    SELECT DISTINCT a.query_id,t.table_id,a.access_type
    FROM raw_query_table_access a
    JOIN dim_tables t USING(catalog_name,schema_name,table_name)
    JOIN fact_queries q ON q.query_id=a.query_id
    """)
    con.execute("""
    CREATE OR REPLACE TABLE agg_daily_table_usage AS
    SELECT CAST(q.started_at AS DATE) AS usage_date,a.table_id,
           COUNT(DISTINCT q.query_id) AS query_count,
           COUNT(DISTINCT q.user_id) AS distinct_users
    FROM fact_queries q JOIN fact_query_table_access a USING(query_id)
    WHERE a.access_type='READ'
    GROUP BY 1,2
    """)
    # Validate membership before attribution; overlapping intervals would double count.
    overlaps = con.execute("""
        SELECT COUNT(*) FROM user_team_history a JOIN user_team_history b
        ON a.user_id=b.user_id AND a.valid_from < b.valid_from
        AND (a.valid_to IS NULL OR a.valid_to > b.valid_from)
    """).fetchone()[0]
    assert overlaps == 0, 'Overlapping user/team membership intervals'
    con.execute("""
    CREATE OR REPLACE TABLE agg_daily_team_usage AS
    WITH attributed AS (
      SELECT q.*, COALESCE(h.team_id,'UNKNOWN') AS team_id
      FROM fact_queries q
      LEFT JOIN user_team_history h
      ON q.user_id=h.user_id AND q.started_at>=h.valid_from
      AND (h.valid_to IS NULL OR q.started_at<h.valid_to)
    )
    SELECT CAST(started_at AS DATE) AS usage_date,team_id,
           COUNT(*) AS query_count,SUM(cpu_time_ms) AS total_cpu_ms,
           SUM(physical_input_bytes) AS total_input_bytes,
           SUM(wall_time_ms) AS total_wall_time_ms,
           MAX(peak_memory_bytes) AS max_peak_memory_bytes
    FROM attributed GROUP BY 1,2
    """)

refresh()
assert con.execute('SELECT COUNT(*) FROM fact_queries').fetchone()[0] == 7
assert con.execute("SELECT cpu_time_ms FROM fact_queries WHERE query_id='q102'").fetchone()[0] == 9000
assert con.execute("SELECT COUNT(*) FROM fact_query_table_access WHERE query_id='q101'").fetchone()[0] == 2
assert con.execute("""SELECT distinct_users FROM agg_daily_table_usage a JOIN dim_tables t USING(table_id)
WHERE usage_date=DATE '2026-10-20' AND table_name='orders'""").fetchone()[0] == 2
assert con.execute("SELECT total_cpu_ms FROM agg_daily_team_usage WHERE usage_date=DATE '2026-10-20' AND team_id='team_B'").fetchone()[0] == 11000
assert con.execute("SELECT total_cpu_ms FROM agg_daily_team_usage WHERE usage_date=DATE '2026-11-02' AND team_id='team_B'").fetchone()[0] == 4000
assert con.execute("SELECT total_cpu_ms FROM agg_daily_team_usage WHERE team_id='UNKNOWN'").fetchone()[0] == 1000
assert con.execute('SELECT SUM(total_cpu_ms) FROM agg_daily_team_usage').fetchone()[0] == con.execute('SELECT SUM(cpu_time_ms) FROM fact_queries').fetchone()[0]
before = con.execute('SELECT SUM(total_cpu_ms) FROM agg_daily_team_usage').fetchone()[0]
refresh()
assert con.execute('SELECT SUM(total_cpu_ms) FROM agg_daily_team_usage').fetchone()[0] == before
print('PASS: 7 unique queries (duplicate q102 resolved)')
print('PASS: q101 has 2 distinct table accesses')
print('PASS: October 20 orders has 2 distinct users')
print('PASS: late q106 included on October 20; team_B CPU = 11,000 ms')
print('PASS: u01 November query attributed to team_B')
print('PASS: unmatched u04 usage assigned to UNKNOWN')
print('PASS: team CPU sums equal query CPU sums')
print('PASS: second refresh does not duplicate data')
print('Daily team usage:')
print(con.execute('SELECT usage_date,team_id,query_count,total_cpu_ms FROM agg_daily_team_usage ORDER BY 1,2').fetchdf().to_string(index=False))
con.close()
