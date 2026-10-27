import duckdb

con = duckdb.connect("meta_dwh_test.duckdb")

result = con.execute("""
SELECT
    COUNT(DISTINCT q.user_id) AS distinct_users
FROM raw_query_events AS q
JOIN raw_query_table_access AS a
    ON q.query_id = a.query_id
WHERE a.table_name = 'orders'
  AND q.started_at >= TIMESTAMP '2026-10-01'
  AND q.started_at < TIMESTAMP '2026-11-01';
""").fetchone()

print("Distinct users querying orders in October:", result[0])

team_usage = con.execute("""
SELECT
    h.team_id,
    COUNT(*) AS query_count,
    SUM(q.cpu_time_ms) / 1000.0 AS cpu_seconds
FROM raw_query_events AS q
JOIN raw_user_team_history AS h
    ON q.user_id = h.user_id
   AND q.started_at >= h.valid_from
   AND (
       h.valid_to IS NULL
       OR q.started_at < h.valid_to
   )
GROUP BY h.team_id
ORDER BY h.team_id;
""").fetchall()

print("\nTeam resource consumption:")

for team_id, query_count, cpu_seconds in team_usage:
    print(team_id, query_count, cpu_seconds)
    
con.close()
