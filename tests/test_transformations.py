import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
# vamos simular um teste de integração Spark local.


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("UnitTests").getOrCreate()


def test_coordinate_cleaning(spark):
    """
    Testa se a função converte '19,90' (PT-BR) para 19.90 (Float).
    """
    # Arrange
    data = [("19,90",), ("-43,123",), (None,)]
    schema = StructType([StructField("coord_raw", StringType(), True)])
    df = spark.createDataFrame(data, schema)

    # Act
    # Usando a lógica que aplicamos no Silver (regexp_replace + cast)
    from pyspark.sql.functions import regexp_replace, col
    from pyspark.sql.types import DoubleType

    df_clean = df.withColumn(
        "coord_float", regexp_replace(col("coord_raw"), ",", ".").cast(DoubleType())
    )

    results = df_clean.collect()

    # Assert
    assert results[0]["coord_float"] == 19.90
    assert results[1]["coord_float"] == -43.123
    assert results[2]["coord_float"] is None


def test_mco_column_normalization(spark):
    """
    Testa se conseguimos padronizar colunas com espaços e acentos.
    """
    # Arrange
    data = [("8103 ", " SC01")]  # Espaços extras
    schema = StructType(
        [
            StructField("LINHA", StringType(), True),
            StructField("COD ", StringType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema)

    # Act - Simula a lógica do MCO
    from pyspark.sql.functions import trim, upper, col

    df_clean = df.select(
        trim(upper(col("LINHA"))).alias("linha_clean"),
        trim(upper(col("COD "))).alias("cod_clean"),
    )

    row = df_clean.collect()[0]

    # Assert
    assert row["linha_clean"] == "8103"
    assert row["cod_clean"] == "SC01"
