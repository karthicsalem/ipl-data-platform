from pyspark.sql import SparkSession

def build_dim_player(spark, player_records):
    from pyspark.sql.functions import collect_set
    df = spark.createDataFrame(player_records,['player_id','player_name'])
    dim_player = df.groupby('player_id')\
                    .agg(collect_set('player_name').alias('name_variants'))
    return dim_player

def build_dim_match(spark:SparkSession):
    return spark.sql("""
        SELECT DISTINCT
            match_id,
            season,
            match_date,
            venue,
            city,
            match_number,
            team_1,
            team_2,
            toss_winner,
            toss_decision,
            match_winner,
            player_of_match,
            win_type,
            win_margin,
            MAX(is_super_over) OVER (PARTITION BY match_id) as has_super_over
        FROM bronze
    """)

def build_fact_delivery(spark:SparkSession):
    return spark.sql("""
        -- Keys
        Select b.match_id,          --FK → dim_match
        p1.player_id player_id_batter,         --FK → dim_player
        p2.player_id player_id_bowler,         --FK → dim_player
        p3.player_id player_id_non_striker,    --FK → dim_player

        -- Positional
        innings_number,
        over_number,
        delivery_index,
        is_legal_delivery,
        is_super_over,

        -- Runs (measurable facts)
        runs_batter,
        runs_extras,
        runs_total,
        extras_wides,
        extras_noballs,
        extras_byes,
        extras_legbyes,

        -- Wicket (measurable facts)
        is_wicket,
        p4.player_id player_id_out,     --FK → dim_player
        dismissal_kind,
        fielder,

        -- Ingestion
        source_file,
        ingested_at
        from bronze b
        left join player_match_registry p1
        on b.match_id = p1.match_id
        and b.batter = p1.player_name
        left join player_match_registry p2
        on b.match_id = p2.match_id
        and b.bowler = p2.player_name        
        left join player_match_registry p3
        on b.match_id = p3.match_id
        and b.non_striker = p3.player_name
        left join player_match_registry p4
        on b.match_id = p4.match_id
        and b.player_out = p4.player_name
    """)