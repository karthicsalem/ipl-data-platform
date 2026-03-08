from transformer import create_spark_session, read_raw_json, flatten_to_bronze, write_bronze
from registry import db_setup, get_unprocessed_files, update_registry, close_connection

if __name__== "__main__":
    conn = db_setup()
    files = get_unprocessed_files(conn)
    if not files:
        print("No new files to process")
        close_connection(conn)
        exit()
    
    spark = create_spark_session()
    raw_df = read_raw_json(spark,files)
    bronze_df = flatten_to_bronze(spark)
    #writing revision data for specific matches overwrites that year's data
    write_bronze(bronze_df)
    update_registry(conn,files)
    close_connection(conn)