from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dbt_silver_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    description="Pipeline para hacer la transformación de datos para la capa silver usando dbt, tambien incluye pruebas de calidad",
    catchup=False,
    tags=["silver"]
) as dag:

    # Tarea para ejecutar dbt run en la capa silver
    run_silver = BashOperator(
        task_id="dbt_run_silver",
        doc="Ejecuta dbt run para transformar los datos en la capa silver.",
        bash_command="cd /dbt && dbt run --select silver"
    )


    # Tarea para ejecutar dbt test en la capa silver
    test_silver = BashOperator(
        task_id="dbt_test_silver",
        doc="Ejecuta dbt test para validar la calidad de los datos transformados en la capa silver.",
        bash_command="cd /dbt && dbt test --select silver"
    )

    # Definición de la secuencia de tareas
    run_silver >> test_silver