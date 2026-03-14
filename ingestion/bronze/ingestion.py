
import sys,os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from registry import (
    db_setup,
    get_unprocessed_files,
    update_registry,
    extract_player_registry,
    close_connection
)
from transformer import (
    create_spark_session,
    read_raw_json,
    flatten_to_bronze,
    write_bronze,
    extract_match_player_registry
)
from transformation.silver.transformer import (
    build_dim_player,
    build_dim_match,
    build_fact_delivery
)

def main():
    # ── Registry & file detection ──────────────────────────
    conn = db_setup()
    files = get_unprocessed_files(conn)

    if not files:
        print("No new files to process")
        close_connection(conn)
        return

    # ── Bronze ─────────────────────────────────────────────
    spark = create_spark_session()
    read_raw_json(spark, files)
    bronze_df = flatten_to_bronze(spark)
    bronze_df.createOrReplaceTempView("bronze")
    write_bronze(bronze_df)
    #writing revision data for specific matches overwrites that year's data
    # ── Silver ─────────────────────────────────────────────
    player_records = extract_player_registry(files)
    dim_player = build_dim_player(spark, player_records)
    dim_player.createOrReplaceTempView('player')
    dim_player.write \
        .mode("overwrite") \
        .parquet("data/silver/dim_player/")

    dim_match = build_dim_match(spark)
    dim_match.write \
        .mode("overwrite") \
        .parquet("data/silver/dim_match/")
    
    extract_match_player_registry(spark)
    fact_delivery = build_fact_delivery(spark)
    try:
        if (not (fact_delivery.count()-bronze_df.count())==0):
            raise Exception("Source data and fact_delivery counts don't match!!!")
    except Exception as e:
        print(e)
    print('bronze:',bronze_df.count())   
    print('fact_delivery:',fact_delivery.count())    
    print('null batters:',fact_delivery.filter("player_id_batter IS NULL").count())

    fact_delivery.write \
        .mode('overwrite') \
        .parquet('data/silver/fact_delivery/')

    # ── Update registry ────────────────────────────────────
    update_registry(conn, files)
    close_connection(conn)

if __name__ == "__main__":
    main()
