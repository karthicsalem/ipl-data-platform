import duckdb

# ── Connections ────────────────────────────────────────────────────────────────
fact_del = duckdb.sql("SELECT * FROM 'data/silver/fact_delivery/*.parquet'")
dim_match = duckdb.sql("SELECT * FROM 'data/silver/dim_match/*.parquet'")
dim_player = duckdb.sql("SELECT * FROM 'data/silver/dim_player/*.parquet'")

duckdb.sql("""select distinct dismissal_kind from fact_del""").show()

bowling_scorecard = duckdb.sql("""
        with base as (
            SELECT             
            f.match_id,
            f.innings_number,
            f.player_id_bowler,
            array_sort(d.name_variants)[1]          AS bowler_name,
            f.batting_team,
            f.bowling_team,
            sum(CAST(is_legal_delivery AS INT)) AS balls_bowled,
            SUM(CASE WHEN runs_total = 0 
            AND is_legal_delivery = true 
            THEN 1 ELSE 0 END)      AS dot_balls,
            CAST(FLOOR(SUM(CAST(is_legal_delivery AS INT)) / 6) AS INTEGER) || '.' || 
            (SUM(CAST(is_legal_delivery AS INT)) % 6)  AS overs_bowled,
            sum(runs_batter + coalesce(extras_wides,0) + coalesce(extras_noballs,0)) as runs_conceded,
            SUM(COALESCE(extras_wides, 0))    AS wides,
            SUM(COALESCE(extras_noballs, 0))  AS noballs,
            SUM(CASE WHEN is_wicket = 1 
            AND dismissal_kind NOT IN ('run out', 'obstructing the field', 'retired hurt')
            THEN 1 ELSE 0 END) AS wickets
        FROM fact_del f
        LEFT JOIN dim_player d
            ON f.player_id_bowler = d.player_id
        GROUP BY
            f.match_id,
            f.innings_number,
            f.player_id_bowler,
            array_sort(d.name_variants)[1],
            f.batting_team,
            f.bowling_team )
        select *,
                ROUND(runs_conceded * 6.0 / NULLIF(balls_bowled, 0), 2) AS economy,
                ROUND(runs_conceded / NULLIF(wickets, 0), 2) AS bowling_average
            from base order by match_id,innings_number                            
""")
bowling_scorecard.show(max_width=1000)