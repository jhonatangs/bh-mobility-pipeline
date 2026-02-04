from include.scripts.spark_utils import get_spark_session
from pyspark.sql.functions import col, to_timestamp, current_timestamp
from pyspark.sql.types import DoubleType


def process_silver():
    spark = get_spark_session("SilverLayer")
    bronze_path = "/opt/airflow/data/bronze/bus_position/*/*.parquet"

    try:
        df = spark.read.parquet(bronze_path)
    except Exception:
        print("No data in bronze yet.")
        return

    df_transformed = (
        df.withColumnRenamed("lat", "latitude")
        .withColumnRenamed("lon", "longitude")
        .withColumn("latitude", col("latitude").cast(DoubleType()))
        .withColumn("longitude", col("longitude").cast(DoubleType()))
        .withColumn(
            "event_timestamp", to_timestamp(col("datahora"), "dd/MM/yyyy HH:mm:ss")
        )
        .withColumn("_processed_at", current_timestamp())
        .dropDuplicates(["numero_do_veiculo", "event_timestamp"])
    )

    silver_path = "/opt/airflow/data/silver/bus_position"
    df_transformed.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(silver_path)
    print(f"Silver layer updated at {silver_path}")
