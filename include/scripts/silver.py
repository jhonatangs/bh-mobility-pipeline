from include.scripts.spark_utils import get_spark_session
from pyspark.sql.functions import (
    col,
    to_timestamp,
    current_timestamp,
    trim,
    upper,
    lit,
    regexp_replace,
    when,
)
from pyspark.sql.types import DoubleType


def process_silver():
    """
    Processa GPS aplicando o Dicionário de Dados da PBH.
    """
    spark = get_spark_session("SilverLayer")
    bronze_path = "/opt/airflow/data/bronze/bus_position/*/*.parquet"

    try:
        df = spark.read.parquet(bronze_path)
    except Exception:
        print("⚠️ Bronze Bus vazia.")
        return

    print(f"📊 Colunas na Bronze: {df.columns}")

    # --- MAPEAMENTO CONFORME DICIONÁRIO ---
    # LT -> Latitude
    # LG -> Longitude
    # NV -> Número Veículo
    # NL -> Código Linha
    # HR -> DataHora (AnoMesDiaHoraMinutoSegundo)

    # 1. Renomeação Segura (verifica se a coluna existe antes)
    mappings = {
        "LT": "latitude",
        "LG": "longitude",
        "NV": "numero_do_veiculo",
        "NL": "cod_linha",
        "HR": "datahora_raw",
    }

    for old_col, new_col in mappings.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)

    # 2. Conversão de Tipos
    # Latitude/Longitude (PBH usa vírgula decimal ex: -19,123)
    if "latitude" in df.columns:
        df = df.withColumn(
            "latitude", regexp_replace(col("latitude"), ",", ".").cast(DoubleType())
        )

    if "longitude" in df.columns:
        df = df.withColumn(
            "longitude", regexp_replace(col("longitude"), ",", ".").cast(DoubleType())
        )

    # Timestamp (Formato YYYYMMDDHHMMSS)
    if "datahora_raw" in df.columns:
        df = df.withColumn(
            "event_timestamp", to_timestamp(col("datahora_raw"), "yyyyMMddHHmmss")
        )
    else:
        # Fallback se não tiver HR
        df = df.withColumn("event_timestamp", current_timestamp())

    df = df.withColumn("_processed_at", current_timestamp())

    # 3. Filtragem e Deduplicação
    required_cols = ["latitude", "longitude", "numero_do_veiculo", "event_timestamp"]
    # Verifica se colunas existem antes de filtrar
    valid_cols = [c for c in required_cols if c in df.columns]

    if len(valid_cols) == 4:
        df_final = df.filter(
            col("latitude").isNotNull() & col("longitude").isNotNull()
        ).dropDuplicates(["numero_do_veiculo", "event_timestamp"])
    else:
        print(
            f"⚠️ Faltando colunas essenciais para Silver: {set(required_cols) - set(df.columns)}"
        )
        df_final = df  # Salva o que tem para debug

    # 4. Salvar
    silver_path = "/opt/airflow/data/silver/bus_position"
    df_final.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(silver_path)
    print(f"✅ Silver BUS salva em: {silver_path}")


def process_mco_silver():
    """
    Processa MCO para extrair Dimensão de Linhas.
    Baseado no Dicionário Oficial: Usamos colunas 'LINHA' e 'CONCESSIONÁRIA'.
    """
    spark = get_spark_session("SilverMCO")
    bronze_path = "/opt/airflow/data/bronze/mco/*/*.parquet"

    try:
        df = spark.read.parquet(bronze_path)
    except Exception:
        print("⚠️ Bronze MCO vazia.")
        return

    print(f"📊 Colunas MCO Bronze: {df.columns}")

    # --- MAPEAMENTO BASEADO NO PDF DO MCO ---
    # Coluna 'LINHA' -> Código da Linha (ex: SC01A)
    # Coluna 'CONCESSIONÁRIA' -> Código do Consórcio (ex: 801)

    # 1. Verifica colunas disponíveis
    has_linha = "LINHA" in df.columns
    has_conc = (
        "CONCESSIONÁRIA" in df.columns or "CONCESSIONARIA" in df.columns
    )  # Sem acento por segurança

    if not has_linha:
        print("❌ Erro Crítico: Coluna 'LINHA' não encontrada no arquivo MCO.")
        # Tenta listar colunas para debug do usuário
        print(f"Colunas disponíveis: {df.columns}")
        return

    # Normaliza nome da coluna de Concessionária (com ou sem acento)
    col_conc = "CONCESSIONÁRIA" if "CONCESSIONÁRIA" in df.columns else "CONCESSIONARIA"

    # 2. Seleciona e Renomeia
    df_dim = df.select(
        col("LINHA").alias("cod_linha"), col(col_conc).alias("consorcio")
    ).distinct()  # Pega apenas linhas únicas, ignorando as milhões de viagens

    # 3. Tratamento
    df_clean = (
        df_dim.withColumn("cod_linha", trim(upper(col("cod_linha"))))
        .withColumn("consorcio", trim(upper(col("consorcio"))))
        .withColumn("nome_linha", lit("N/A - Ver MCO"))
    )  # Placeholder pois MCO não tem nome descritivo

    # 4. Salva Silver
    silver_path = "/opt/airflow/data/silver/mco"
    df_clean.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(silver_path)
    print(f"✅ Silver MCO (Dimensão Extraída) salva em {silver_path}")
