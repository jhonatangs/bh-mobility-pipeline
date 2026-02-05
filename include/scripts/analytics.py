import duckdb


def run_analytics_query():
    """
    Valida a entrega final consultando a camada Gold com DuckDB.
    """
    con = duckdb.connect()

    # Instala extensão Delta (necessária para ler o formato)
    try:
        con.execute("INSTALL delta;")
        con.execute("LOAD delta;")
    except Exception as e:
        print(f"⚠️ Aviso DuckDB: {e}")

    print(">>> RELATÓRIO DE MOBILIDADE (DUCKDB) <<<")

    # CAMINHO CORRIGIDO AQUI:
    table_path = "/opt/airflow/data/gold/mobility_analytics"

    # Query demonstrando o Join (trazendo o Consórcio)
    query = f"""
    SELECT 
        cod_linha, 
        consorcio,
        total_pings, 
        last_seen 
    FROM delta_scan('{table_path}')
    ORDER BY total_pings DESC
    LIMIT 10;
    """

    try:
        # Executa e converte para Pandas para visualizar bonito no log
        result = con.execute(query).df()

        print("\n--- TOP 10 LINHAS MAIS ATIVAS ---")
        print(result)
        print("---------------------------------\n")

        if result.empty:
            raise ValueError(
                "❌ A query retornou vazia! Verifique se a camada Gold foi populada."
            )

        print("✅ Validação Analytics concluída com sucesso.")

    except Exception as e:
        print(f"❌ Erro ao consultar Data Warehouse: {e}")
        # Dica de debug no erro
        if "Path does not exist" in str(e):
            print(
                f"DICA: Verifique se a pasta {table_path} foi criada pela task gold_enrichment."
            )
        raise
