import duckdb

con = duckdb.connect("meta_dwh_test.duckdb")

con.execute("""
CREATE OR REPLACE TABLE raw_query_events AS
SELECT *
FROM (
    VALUES
        ('q101', 'u01', TIMESTAMP '2026-10-20 10:00:00', 5000),
        ('q102', 'u02', TIMESTAMP '2026-10-20 11:00:00', 9000),
        ('q103', 'u01', TIMESTAMP '2026-10-21 12:00:00', 3000),
        ('q104', 'u03', TIMESTAMP '2026-10-21 13:00:00', 7000),
        ('q105', 'u01', TIMESTAMP '2026-11-02 09:00:00', 4000)
) AS t(query_id, user_id, started_at, cpu_time_ms);
""")

con.execute("""
CREATE OR REPLACE TABLE raw_query_table_access AS
SELECT *
FROM (
    VALUES
        ('q101', 'orders'),
        ('q101', 'customers'),
        ('q102', 'orders'),
        ('q103', 'orders'),
        ('q104', 'payments'),
        ('q105', 'orders')
) AS t(query_id, table_name);
""")

con.execute("""
CREATE OR REPLACE TABLE raw_user_team_history AS
SELECT *
FROM (
    VALUES
        ('u01', 'team_A',
         TIMESTAMP '2026-10-01',
         TIMESTAMP '2026-11-01'),

        ('u01', 'team_B',
         TIMESTAMP '2026-11-01',
         NULL),

        ('u02', 'team_B',
         TIMESTAMP '2026-10-01',
         NULL),

        ('u03', 'team_A',
         TIMESTAMP '2026-10-01',
         NULL)
) AS t(user_id, team_id, valid_from, valid_to);
""")

print("Test data created successfully.")

for table in [
    "raw_query_events",
    "raw_query_table_access",
    "raw_user_team_history"
]:
    count = con.execute(
        f"SELECT COUNT(*) FROM {table}"
    ).fetchone()[0]

    print(f"{table}: {count} rows")

con.close()
