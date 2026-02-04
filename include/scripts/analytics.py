import duckdb


def run_analytics_query():
    con = duckdb.connect()
    con.execute("INSTALL delta;")
    con.execute("LOAD delta;")

    print(">>> DUCKDB ANALYTICS REPORT <<<")
    query = """
    SELECT cod_linha, total_pings, last_seen 
    FROM delta_scan('/opt/airflow/data/gold/line_analytics')
    ORDER BY total_pings DESC LIMIT 5;
    """
    try:
        result = con.execute(query).df()
        print(result)
        if result.empty:
            raise ValueError("Empty resultset in Gold")
    except Exception as e:
        print(f"Analytics error: {e}")
        raise
