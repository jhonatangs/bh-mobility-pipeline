from include.scripts.spark_utils import get_spark_session
from pyspark.sql.functions import count, max, min, col, avg, lit


def process_gold():
    """
    Realiza o JOIN entre Fatos (GPS) e Dimensões (MCO) para criar a tabela final.
    """
    spark = get_spark_session("GoldLayer")

    # Caminhos de Entrada
    silver_bus_path = "/opt/airflow/data/silver/bus_position"
    silver_mco_path = "/opt/airflow/data/silver/mco"

    # Caminho de Saída (O Novo Nome Correto)
    gold_path = "/opt/airflow/data/gold/mobility_analytics"

    try:
        df_bus = spark.read.format("delta").load(silver_bus_path)
    except Exception:
        print("❌ Erro: Tabela Silver BUS não encontrada.")
        return

    try:
        df_mco = spark.read.format("delta").load(silver_mco_path)
    except Exception:
        print("⚠️ Tabela Silver MCO não encontrada. Seguindo apenas com dados de GPS.")
        df_mco = None

    print(f"📊 Linhas de GPS para processar: {df_bus.count()}")

    # Realiza o Join se o MCO existir
    if df_mco:
        # Left Join para não perder ônibus que não tenham cadastro no MCO
        df_joined = df_bus.join(df_mco, on="cod_linha", how="left")
    else:
        df_joined = df_bus.withColumn("consorcio", lit("Desconhecido")).withColumn(
            "nome_linha", lit("Desconhecido")
        )

    # Agregação de Negócio
    # Ex: Qual consórcio tem mais ônibus rodando agora?
    df_agg = df_joined.groupBy("cod_linha", "consorcio").agg(
        count("numero_do_veiculo").alias("total_pings"),
        max("event_timestamp").alias("last_seen"),
        min("latitude").alias("min_lat"),
        max("latitude").alias("max_lat"),
    )

    # Escrita Otimizada (ZORDER para deixar consultas rápidas no DuckDB)
    df_agg.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(gold_path)

    # Tenta otimizar (pode falhar em local mode dependendo da versão do delta, então usamos try)
    try:
        spark.sql(f"OPTIMIZE delta.`{gold_path}` ZORDER BY (cod_linha)")
    except:
        pass

    print(f"✅ Gold gerada com sucesso em: {gold_path}")
