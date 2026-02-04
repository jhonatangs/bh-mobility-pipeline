from include.scripts.spark_utils import get_spark_session
from pyspark.sql.functions import count, max, min, col


def process_gold():
    spark = get_spark_session("GoldLayer")
    silver_path = "/opt/airflow/data/silver/bus_position"

    try:
        df = spark.read.format("delta").load(silver_path)
    except Exception:
        print("Silver data not found.")
        return

    df_agg = df.groupBy("cod_linha").agg(
        count("numero_do_veiculo").alias("total_pings"),
        max("event_timestamp").alias("last_seen"),
        min("latitude").alias("min_lat"),
        max("latitude").alias("max_lat"),
    )

    gold_path = "/opt/airflow/data/gold/line_analytics"
    df_agg.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(gold_path)
    print(f"Gold layer updated at {gold_path}")
