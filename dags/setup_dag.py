from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# Función principal del task
def create_postgres_schemas():
    from sqlalchemy import create_engine, text
    db_url = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
    if not db_url:
        raise ValueError("La variable de entorno AIRFLOW__DATABASE__SQL_ALCHEMY_CONN no está definida.")
    
    engine = create_engine(db_url)
    with engine.connect() as conn:
        for schema in ["bronze", "silver", "gold"]:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
            print(f"✅ Esquema '{schema}' verificado o creado.")

# Argumentos por defecto del DAG
default_args = {
    "owner": "qversity",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Definición del DAG
with DAG(
    dag_id="setup_dag",
    default_args=default_args,
    description="Inicializa los esquemas bronze, silver y gold en PostgreSQL",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["setup", "postgres"]
) as dag:

    create_schemas_task = PythonOperator(
        task_id="create_postgres_schemas",
        python_callable=create_postgres_schemas
    )
