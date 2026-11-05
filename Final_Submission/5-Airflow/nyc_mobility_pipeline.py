from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def start_pipeline():
    print("NYC Mobility ETL Pipeline Started")


def validate_postgres():
    hook = PostgresHook(postgres_conn_id="qeema_postgres")

    result = hook.get_first(
        "SELECT COUNT(*) FROM public.fact_trips;"
    )

    count = result[0]
    print(f"PostgreSQL connection successful.")
    print(f"Total records in fact_trips: {count:,}")

    if count <= 0:
        raise ValueError("fact_trips table is empty")


with DAG(
    dag_id="nyc_mobility_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["NYC", "Mobility", "ETL"],
) as dag:

    start = PythonOperator(
        task_id="start_pipeline",
        python_callable=start_pipeline,
    )

    validate = PythonOperator(
        task_id="validate_postgres",
        python_callable=validate_postgres,
    )

    start >> validate
