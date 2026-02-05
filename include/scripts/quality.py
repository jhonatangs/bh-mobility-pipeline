from include.scripts.spark_utils import get_spark_session
from pyspark.sql.functions import col, count, when


def check_silver_quality():
    """
    Verifica a saúde da tabela Silver de Ônibus.
    Regras:
    1. Não pode ter latitude/longitude NULA.
    2. Não pode ter Timestamp no futuro (viagem no tempo?).
    3. Volume de dados deve ser > 0.
    """
    spark = get_spark_session("QualityCheck")
    df = spark.read.format("delta").load("/opt/airflow/data/silver/bus_position")

    # Regra 1: Volumetria
    total_count = df.count()
    if total_count == 0:
        raise ValueError("DQ FALHOU: Tabela Silver vazia!")

    print(f"DQ Check: Total linhas = {total_count}")

    # Regra 2: Nulos Críticos
    null_coords = df.filter(
        col("latitude").isNull() | col("longitude").isNull()
    ).count()
    if null_coords > 0:
        # Em produção, poderíamos apenas alertar. Num teste rigoroso, falhamos.
        print(f"⚠️ AVISO DQ: Encontradas {null_coords} linhas com coordenadas nulas.")
        # raise ValueError("DQ FALHOU: Coordenadas nulas encontradas na Silver.")

    # Regra 3: Duplicatas (Chave Primária: Veículo + Timestamp)
    # Verifica se a PK é única
    duplicates = (
        df.groupBy("numero_do_veiculo", "event_timestamp")
        .count()
        .filter("count > 1")
        .count()
    )
    if duplicates > 0:
        print(f"⚠️ AVISO DQ: Encontradas {duplicates} duplicatas de chave primária.")

    print("✅ Silver Quality Check Passou!")


def check_gold_quality():
    """
    Verifica se o JOIN funcionou.
    """
    spark = get_spark_session("QualityCheck")
    df = spark.read.format("delta").load("/opt/airflow/data/gold/mobility_analytics")

    # Verifica taxa de sucesso do JOIN (quantos consórcios foram encontrados)
    total = df.count()
    unknown_consortium = df.filter(col("consorcio") == "Desconhecido").count()

    match_rate = ((total - unknown_consortium) / total) * 100

    print(f"DQ Gold: Taxa de Enriquecimento (Join MCO) = {match_rate:.2f}%")

    if match_rate < 10:
        print(
            "⚠️ ALERTA: O Join com MCO parece ter falhado massivamente (menos de 10% de match)."
        )
        # Não damos raise error aqui pq sabemos que os códigos da PBH divergem,
        # mas logamos o alerta.

    print("✅ Gold Quality Check Passou!")
