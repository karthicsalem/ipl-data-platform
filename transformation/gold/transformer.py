from pyspark.sql import SparkSession


class GoldTransformer:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._register_silver_views()

    def _register_silver_views(self):
        """Register all Silver parquet files as Spark temp views"""
        views = {
            "fact_del": "data/silver/fact_delivery",
            "dim_match": "data/silver/dim_match",
            "dim_player": "data/silver/dim_player",
            "match_player_registry": "data/silver/match_player_registry",
        }
        for view_name, path in views.items():
            if path:
                self.spark.read.parquet(path).createOrReplaceTempView(view_name)
                print(f"Registered view: {view_name}")

    def build_batting_scorecard(self):
        batting_scorecard = self.spark.sql("""
        WITH base AS (
            SELECT
                f.match_id,
                f.innings_number,
                f.player_id_batter,
                array_sort(d.name_variants)[0]          AS batter_name,
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
                array_sort(d.name_variants)[0],
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
        batting_scorecard.createOrReplaceTempView("batting_scorecard")
        return batting_scorecard

    def build_bowling_scorecard(self):
        bowling_scorecard = self.spark.sql("""
            with base as (
                SELECT
                f.match_id,
                f.innings_number,
                f.player_id_bowler,
                array_sort(d.name_variants)[0]          AS bowler_name,
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
                array_sort(d.name_variants)[0],
                f.batting_team,
                f.bowling_team )
            select *,
                    ROUND(runs_conceded * 6.0 / NULLIF(balls_bowled, 0), 2) AS economy,
                    ROUND(runs_conceded / NULLIF(wickets, 0), 2) AS bowling_average
                from base order by match_id,innings_number
            """)
        bowling_scorecard.createOrReplaceTempView("bowling_scorecard")
        return bowling_scorecard

    def build_team_match_summary(self):
        return self.spark.sql("""
            WITH innings_totals AS (
        SELECT
            match_id,
            innings_number,
            batting_team,
            bowling_team,
            SUM(runs_total)                                          AS total_runs,
            SUM(CASE WHEN is_wicket = 1 THEN 1 ELSE 0 END)          AS total_wickets,
            SUM(CAST(is_legal_delivery AS INT))                                   AS total_balls,
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
            """)

    def build_player_season_stats(self):
        return self.spark.sql("""
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
        ORDER BY pm.season, pm.matches_played DESC""")

    def write_gold(self, df, table_name, partition_col="season"):
        df.write.mode("overwrite").partitionBy(partition_col).parquet(f"data/gold/{table_name}/")
        print(f"Written gold table: {table_name}")
