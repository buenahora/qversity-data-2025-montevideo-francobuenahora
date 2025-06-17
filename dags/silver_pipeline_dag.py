from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dbt_silver_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["silver"]
) as dag:

    run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command="cd /dbt && dbt run --select silver"
    )

    test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command="cd /dbt && dbt test --select silver"
    )

    run_silver >> test_silver