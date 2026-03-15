import duckdb
from batting_score_card import batting_scorecard
from bowling_score_card import bowling_scorecard
# ── Connections ────────────────────────────────────────────────────────────────
fact_del = duckdb.sql("SELECT * FROM 'data/silver/fact_delivery/*.parquet'")
dim_match = duckdb.sql("SELECT * FROM 'data/silver/dim_match/*.parquet'")
dim_player = duckdb.sql("SELECT * FROM 'data/silver/dim_player/*.parquet'")
match_player_registry = duckdb.sql("SELECT * FROM 'data/silver/match_player_registry/*.parquet'")

print(batting_scorecard.columns)
print(bowling_scorecard.columns)

player_season_stats = duckdb.sql("""
    WITH player_matches AS (
        SELECT
            r.player_id,
            m.season,
            COUNT(DISTINCT r.match_id)              AS matches_played
        FROM match_player_registry r
        LEFT JOIN dim_match m ON r.match_id = m.match_id
        GROUP BY r.player_id, m.season
    ),
    batting_stats AS (
        SELECT
            bs.player_id_batter,
            bs.batter_name,
            m.season,
            bs.batting_team                         AS team,
            COUNT(DISTINCT bs.match_id)             AS innings,
            SUM(bs.runs)                            AS runs,
            MAX(bs.runs)                            AS highest_score,
            SUM(bs.balls_faced)                     AS balls_faced,
            SUM(bs.fours)                           AS fours,
            SUM(bs.sixes)                           AS sixes,
            SUM(bs.dot_balls)                       AS dot_balls,
            SUM(bs.not_out)                         AS not_outs,
            SUM(CASE WHEN bs.runs >= 50 
                AND bs.runs < 100 THEN 1 ELSE 0 END) AS fifties,
            SUM(CASE WHEN bs.runs >= 100 
                THEN 1 ELSE 0 END)                  AS hundreds
        FROM batting_scorecard bs
        LEFT JOIN dim_match m ON bs.match_id = m.match_id
        GROUP BY bs.player_id_batter, bs.batter_name, m.season, bs.batting_team
    ),
    bowling_stats AS (
        SELECT
            bs.player_id_bowler,
            bs.bowler_name,
            m.season,
            bs.bowling_team                         AS team,
            COUNT(DISTINCT bs.match_id)             AS innings,
            SUM(bs.runs_conceded)                   AS runs_conceded,
            SUM(bs.balls_bowled)                    AS balls_bowled,
            CAST(FLOOR(SUM(bs.balls_bowled)/6) 
                AS INTEGER) || '.' || 
                (SUM(bs.balls_bowled) % 6)          AS overs_bowled,
            SUM(bs.wides)                           AS wides,
            SUM(bs.noballs)                         AS noballs,
            SUM(bs.dot_balls)                       AS dot_balls,
            SUM(bs.wickets)                         AS wickets
        FROM bowling_scorecard bs
        LEFT JOIN dim_match m ON bs.match_id = m.match_id
        GROUP BY bs.player_id_bowler, bs.bowler_name, m.season, bs.bowling_team
    )
    SELECT
        pm.player_id,
        pm.season,
        pm.matches_played,
        COALESCE(ba.batter_name, bo.bowler_name)    AS player_name,
        COALESCE(ba.team, bo.team)                  AS team,

        -- Batting
        ba.innings                                  AS innings_batted,
        ba.runs,
        ba.highest_score,
        ba.balls_faced,
        ba.fours,
        ba.sixes,
        ba.dot_balls                                AS dot_balls_faced,
        ba.not_outs,
        ba.fifties,
        ba.hundreds,
        ROUND(ba.runs * 100.0 / 
            NULLIF(ba.balls_faced, 0), 2)           AS strike_rate,
        CASE WHEN (ba.innings - ba.not_outs) = 0 
        THEN ba.runs
        ELSE ROUND(ba.runs / (ba.innings - ba.not_outs), 2)
        END AS batting_average,

        -- Bowling
        bo.innings                                  AS innings_bowled,
        bo.runs_conceded,
        bo.balls_bowled,
        bo.overs_bowled,
        bo.wides,
        bo.noballs,
        bo.dot_balls                                AS dot_balls_bowled,
        bo.wickets,
        ROUND(bo.runs_conceded * 6.0 / 
            NULLIF(bo.balls_bowled, 0), 2)          AS economy_rate,
        ROUND(bo.runs_conceded / 
            NULLIF(bo.wickets, 0), 2)               AS bowling_average

    FROM player_matches pm
    LEFT JOIN batting_stats ba
        ON pm.player_id = ba.player_id_batter
        AND pm.season = ba.season
    LEFT JOIN bowling_stats bo
        ON pm.player_id = bo.player_id_bowler
        AND pm.season = bo.season
    ORDER BY pm.season, pm.matches_played DESC
""")

player_season_stats.show(max_width=1000)

# Check Virat Kohli's 2016 season — should show ~973 runs
duckdb.sql("""
    SELECT * FROM player_season_stats
    WHERE player_name LIKE '%Kohli%'
    AND season = '2016'
""").show(max_width=1000)