from pyspark.sql import SparkSession


def create_spark_session():
    spark = SparkSession.builder.master("local[*]").appName("ingesion").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def read_raw_json(spark, files):
    raw_df = spark.read.option("multiLine", True).json(files)
    raw_df.createOrReplaceTempView("row_wise")
    return raw_df


def extract_match_player_registry(spark):
    player_registry_df = spark.sql("""
        WITH registry AS (
            SELECT
                regexp_extract(input_file_name(), '([^/]+)\\.json$', 1) AS match_id,
                player_name,
                player_id,
                team
            FROM row_wise rw
            -- explode registry into name/id pairs
            LATERAL VIEW explode(
                from_json(to_json(info.registry.people), 'map<string,string>')
            ) AS player_name, player_id
            -- explode players into team/squad pairs
            LATERAL VIEW explode(
                from_json(to_json(info.players), 'map<string,array<string>>')
            ) AS team, squad
            -- only keep registry entries that appear in a team squad
            WHERE array_contains(squad, player_name)
        )
        SELECT DISTINCT match_id, player_name, player_id, team
        FROM registry
        """)
    player_registry_df.createOrReplaceTempView("player_match_registry")

    player_registry_df.write.mode("overwrite").parquet(
        "data/silver/match_player_registry/"
    )
    print(f"Player match registry built: {player_registry_df.count()} records")


def flatten_to_bronze(spark):
    bronze_df = spark.sql("""
    SELECT
        -- Meta
        meta.data_version                                    AS meta_version,
        meta.created                                         AS meta_created,

        -- Match
        regexp_extract(input_file_name(), '([^/]+)\\.json$', 1) AS match_id,
        info.season                                          AS season,
        info.dates[0]                                        AS match_date,
        info.venue                                           AS venue,
        info.city                                            AS city,
        info.event.match_number                              AS match_number,
        info.teams[0]                                        AS team_1,
        info.teams[1]                                        AS team_2,
        info.toss.winner                                     AS toss_winner,
        info.toss.decision                                   AS toss_decision,
        info.outcome.winner                                  AS match_winner,
        info.player_of_match[0]                              AS player_of_match,
        CASE WHEN info.outcome.by.runs is not null then 'run'
             WHEN info.outcome.by.wickets is not null then 'wickets'
             WHEN info.outcome.result = 'tie' THEN 'tie' 
             ELSE 'unknown'
             END AS win_type,
        COALESCE (info.outcome['by']['runs'],info.outcome['by']['wickets'],0) AS win_margin,
        -- Innings & Over
        innings_pos + 1                                      AS innings_number,
        inning.team                                          AS batting_team,
        CASE when inning.team=info.teams[0] then info.teams[1]
        else info.teams[0] end                               AS bowling_team,
        -- Delivery
        delivery_pos                                         AS delivery_index,
        delivery.batter                                      AS batter,
        delivery.bowler                                      AS bowler,
        delivery.non_striker                                 AS non_striker,
        CASE WHEN delivery.extras.wides IS NULL
        and delivery.extras.noballs IS NULL THEN true
        else false END AS is_legal_delivery,
        -- Runs
        over_data.over                                       AS over_number,
        delivery.runs.batter                                 AS runs_batter,
        delivery.runs.extras                                 AS runs_extras,
        delivery.runs.total                                  AS runs_total,

        -- Extras (all nullable)
        delivery.extras.wides                                AS extras_wides,
        delivery.extras.noballs                              AS extras_noballs,
        delivery.extras.byes                                 AS extras_byes,
        delivery.extras.legbyes                              AS extras_legbyes,

        -- Wicket (all nullable)
        CASE WHEN delivery.wickets IS NOT NULL THEN 1 
             ELSE 0 END                                  AS is_wicket,
        delivery.wickets[0].player_out                       AS player_out,
        delivery.wickets[0].kind                             AS dismissal_kind,
        delivery.wickets[0].fielders[0].name                 AS fielder,

        -- Miscellaneous
        CASE WHEN inning.super_over IS NULL THEN 0 ELSE 1 END AS is_super_over,
                        

        -- Ingestion metadata
        regexp_extract(input_file_name(), '([^/]+)\\.json$', 1)||'.json'    AS source_file,
        current_timestamp()                                  AS ingested_at

    FROM row_wise rw
    LATERAL VIEW posexplode(innings)            AS innings_pos, inning
    LATERAL VIEW posexplode(inning.overs)      AS over_pos,over_data
    LATERAL VIEW posexplode(over_data.deliveries)    AS delivery_pos,delivery
    """)
    return bronze_df


def write_bronze(bronze_df):
    bronze_df.write.mode("overwrite").partitionBy("season").parquet(
        "data/bronze/deliveries/"
    )
    return None
