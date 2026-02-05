from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from include.scripts.bronze import extract_bus_data, extract_mco_data
from include.scripts.silver import process_silver, process_mco_silver
from include.scripts.gold import process_gold
from include.scripts.analytics import run_analytics_query
from include.scripts.quality import (
    check_silver_quality,
    check_gold_quality,
)

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    "etl_urban_mobility_bh",
    default_args=default_args,
    description="Pipeline Completo com Data Quality",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["mobility", "bh", "quality"],
) as dag:
    # --- INGESTÃO ---
    t_bronze_bus = PythonOperator(
        task_id="bronze_bus", python_callable=extract_bus_data
    )
    t_bronze_mco = PythonOperator(
        task_id="bronze_mco", python_callable=extract_mco_data
    )

    # --- PROCESSAMENTO SILVER ---
    t_silver_bus = PythonOperator(task_id="silver_bus", python_callable=process_silver)
    t_silver_mco = PythonOperator(
        task_id="silver_mco", python_callable=process_mco_silver
    )

    # --- QUALITY CHECK SILVER (NOVO) ---
    # Só deixa ir para a Gold se a Silver estiver saudável
    t_dq_silver = PythonOperator(
        task_id="dq_silver_check", python_callable=check_silver_quality
    )

    # --- GOLD ---
    t_gold = PythonOperator(task_id="gold_enrichment", python_callable=process_gold)

    # --- QUALITY CHECK GOLD (NOVO) ---
    t_dq_gold = PythonOperator(
        task_id="dq_gold_check", python_callable=check_gold_quality
    )

    # --- ANALYTICS ---
    t_analytics = PythonOperator(
        task_id="analytics_check", python_callable=run_analytics_query
    )

    # Fluxo Atualizado:
    # Bronze -> Silver -> DQ Check -> Gold -> DQ Check -> Analytics

    t_bronze_bus >> t_silver_bus
    t_bronze_mco >> t_silver_mco

    [t_silver_bus, t_silver_mco] >> t_dq_silver >> t_gold

    t_gold >> t_dq_gold >> t_analytics
