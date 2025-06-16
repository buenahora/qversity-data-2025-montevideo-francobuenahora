from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import os

# Parámetros de conexión a PostgreSQL desde Airflow
sqlalchemy_url = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")

def create_schemas():
    engine = create_engine(sqlalchemy_url)
    with engine.connect() as conn:
        for schema in ["bronze", "silver", "gold"]:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
            print(f"Esquema '{schema}' verificado/creado.")

default_args = {
    "owner": "qversity",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "setup_dag",
    default_args=default_args,
    description="Crea los esquemas bronze, silver y gold en PostgreSQL",
    schedule_interval=None,  # Solo ejecución manual
    catchup=False,
)

create_schemas = PythonOperator(
    task_id="create_schemas",
    python_callable=create_schemas,
    dag=dag
)

