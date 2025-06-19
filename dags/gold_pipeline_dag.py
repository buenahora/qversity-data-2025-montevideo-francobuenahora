from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dbt_gold_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["gold"]
) as dag:

    run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command="cd /dbt && dbt run --select gold"
    )

    test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command="cd /dbt && dbt test --select gold"
    )

    run_gold >> test_gold