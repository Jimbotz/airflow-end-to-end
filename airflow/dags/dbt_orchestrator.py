from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'run_dbt_models',
    default_args=default_args,
    description='Un DAG para ejecutar las transformaciones de dbt en Postgres',
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2026, 3, 8),
    catchup=False,
    tags=['dbt', 'transformacion', 'cdc'],
) as dag:

    # Agregamos --profiles-dir . a ambos comandos
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command='cd /opt/airflow/dbt_project && dbt debug --profiles-dir .', 
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir .',
    )

    dbt_debug >> dbt_run