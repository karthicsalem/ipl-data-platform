from pyspark.sql import SparkSession

def create_spark_session():
    spark = SparkSession.builder.master("local[*]").appName("ingesion").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark

def read_raw_json(spark,files):
    raw_df=spark.read.option("multiLine", True).json(files)
    raw_df.createOrReplaceTempView("row_wise")
    return raw_df

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

        -- Innings & Over
        innings_pos + 1                                      AS innings_number,
        inning.team                                      AS batting_team,

        -- Delivery
        delivery_pos                                         AS delivery_index,
        delivery.batter                                      AS batter,
        delivery.bowler                                      AS bowler,
        delivery.non_striker                                 AS non_striker,
        CASE WHEN delivery.extras.wides IS NULL
        and delivery.extras.noballs IS NULL THEN true
        else false END AS legal_ball,
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
    bronze_df.write \
        .mode("overwrite") \
        .partitionBy("season") \
        .parquet("data/bronze/deliveries/")
    return None
