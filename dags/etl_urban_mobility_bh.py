from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Importe as novas funções
from include.scripts.bronze import extract_bus_data, extract_mco_data
from include.scripts.silver import process_silver, process_mco_silver
from include.scripts.gold import process_gold
from include.scripts.analytics import run_analytics_query

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    "etl_urban_mobility_bh",
    default_args=default_args,
    description="Pipeline Completo (Fato + Dimensão)",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["mobility", "bh", "medallion"],
) as dag:
    # --- Grupo FATO (Bus GPS) ---
    t_bronze_bus = PythonOperator(
        task_id="bronze_bus", python_callable=extract_bus_data
    )
    t_silver_bus = PythonOperator(task_id="silver_bus", python_callable=process_silver)

    # --- Grupo DIMENSÃO (MCO) ---
    t_bronze_mco = PythonOperator(
        task_id="bronze_mco", python_callable=extract_mco_data
    )
    t_silver_mco = PythonOperator(
        task_id="silver_mco", python_callable=process_mco_silver
    )

    # --- Convergência (Gold & Analytics) ---
    t_gold = PythonOperator(task_id="gold_enrichment", python_callable=process_gold)
    t_analytics = PythonOperator(
        task_id="analytics_check", python_callable=run_analytics_query
    )

    # Definindo Fluxo:
    # 1. Roda Bronze Bus e Bronze MCO em paralelo
    # 2. Quando cada Bronze acaba, roda sua respectiva Silver
    # 3. SÓ QUANDO AS DUAS SILVERS ACABAREM, roda a Gold

    t_bronze_bus >> t_silver_bus >> t_gold
    t_bronze_mco >> t_silver_mco >> t_gold

    t_gold >> t_analytics
