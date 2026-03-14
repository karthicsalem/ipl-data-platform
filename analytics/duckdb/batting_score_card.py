import duckdb

# ── Connections ────────────────────────────────────────────────────────────────
fact_del = duckdb.sql("SELECT * FROM 'data/silver/fact_delivery/*.parquet'")
dim_match = duckdb.sql("SELECT * FROM 'data/silver/dim_match/*.parquet'")
dim_player = duckdb.sql("SELECT * FROM 'data/silver/dim_player/*.parquet'")

# ── Batting Scorecard ──────────────────────────────────────────────────────────
batting_scorecard = duckdb.sql("""
    WITH base AS (
        SELECT
            f.match_id,
            f.innings_number,
            f.player_id_batter,
            array_sort(d.name_variants)[1]          AS batter_name,
            f.batting_team,
            f.bowling_team,
            MIN(over_number * 6 + delivery_index) AS first_ball,
            -- Scoring
            SUM(f.runs_batter)                      AS runs,
            SUM(CASE WHEN f.extras_wides IS NULL
                THEN 1 ELSE 0 END)                  AS balls_faced,
            SUM(CASE WHEN f.runs_batter = 4
                THEN 1 ELSE 0 END)                  AS fours,
            SUM(CASE WHEN f.runs_batter = 6
                THEN 1 ELSE 0 END)                  AS sixes,
            SUM(CASE WHEN f.runs_batter = 0
                AND f.extras_wides IS NULL
                THEN 1 ELSE 0 END)                  AS dot_balls,

            -- Dismissal
            CASE WHEN MAX(f.is_wicket) = 0
                THEN 1 ELSE 0 END                   AS not_out,
            MAX(CASE WHEN f.is_wicket = 1
                THEN f.dismissal_kind END)           AS dismissal_kind

        FROM fact_del f
        LEFT JOIN dim_player d
            ON f.player_id_batter = d.player_id
        GROUP BY
            f.match_id,
            f.innings_number,
            f.player_id_batter,
            array_sort(d.name_variants)[1],
            f.batting_team,
            f.bowling_team  
    )
    SELECT
        *,
                RANK() OVER (
            PARTITION BY match_id, innings_number 
            ORDER BY first_ball
        ) AS batting_position,
        ROUND(runs * 100.0 / NULLIF(balls_faced, 0), 2) AS strike_rate
    FROM base
    ORDER BY match_id, innings_number, batting_position
""")

batting_scorecard.show(max_width=1000)