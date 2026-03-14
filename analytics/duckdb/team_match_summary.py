import duckdb

# ── Connections ────────────────────────────────────────────────────────────────
fact_del = duckdb.sql("SELECT * FROM 'data/silver/fact_delivery/*.parquet'")
dim_match = duckdb.sql("SELECT * FROM 'data/silver/dim_match/*.parquet'")
dim_player = duckdb.sql("SELECT * FROM 'data/silver/dim_player/*.parquet'")

duckdb.sql('select distinct win_type from dim_match').show()
# ── Match Summary ──────────────────────────────────────────────────────────
match_summary = duckdb.sql(
    """
    WITH innings_totals AS (
    SELECT
        match_id,
        innings_number,
        batting_team,
        bowling_team,
        SUM(runs_total)                                          AS total_runs,
        SUM(CASE WHEN is_wicket = 1 THEN 1 ELSE 0 END)          AS total_wickets,
        SUM(is_legal_delivery)                                   AS total_balls,
        SUM(COALESCE(extras_wides,0) + COALESCE(extras_noballs,0) 
            + COALESCE(extras_byes,0) + COALESCE(extras_legbyes,0)) AS total_extras
    FROM fact_del
    GROUP BY match_id, innings_number, batting_team, bowling_team
    )
    SELECT
    i.*,m.win_type,
    CAST(FLOOR(total_balls / 6) AS INTEGER) || '.' || 
        (total_balls % 6)                                        AS overs_played,
    ROUND(total_runs * 6.0 / NULLIF(total_balls, 0), 2)         AS run_rate,

    -- Target: first innings total + 1, only relevant for innings 2
    CASE WHEN innings_number % 2 = 0 
        AND win_type != 'unknown'
    THEN LAG(total_runs) OVER (
            PARTITION BY i.match_id ORDER BY innings_number
        ) + 1
    ELSE NULL END AS target
    FROM innings_totals i
    left join dim_match m
    on i.match_id = m.match_id
    ORDER BY i.match_id, innings_number
    """
).show()