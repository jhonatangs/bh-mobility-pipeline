from pyspark.sql import SparkSession


def get_spark_session(app_name="BHMobilityETL"):
    """
    Creates a SparkSession configured for Delta Lake and Local File System.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.driver.memory", "1g")
        .master("local[*]")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
