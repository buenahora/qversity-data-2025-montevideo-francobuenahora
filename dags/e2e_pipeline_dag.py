from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta
import json
import os
import requests
from sqlalchemy import create_engine, text
import requests

# Función principal del task
def create_postgres_schemas():
    '''
    Crea los esquemas bronze, silver y gold en PostgreSQL si no existen.
    Necesita que la variable de entorno AIRFLOW__DATABASE__SQL_ALCHEMY_CONN esté configurada.
    '''
    
    db_url = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
    if not db_url:
        raise ValueError("La variable de entorno AIRFLOW__DATABASE__SQL_ALCHEMY_CONN no está definida.")
    
    engine = create_engine(db_url)
    with engine.connect() as conn:
        for schema in ["bronze", "silver", "gold"]:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
            print(f"Esquema '{schema}' verificado o creado.")

def download_json():
    '''
    Descarga un archivo JSON desde S3 y lo guarda localmente.
    Utiliza la URL pública del bucket S3 de Qversity para obtener el dataset.
    '''
    s3_url = "https://qversity-raw-public-data.s3.amazonaws.com/mobile_customers_messy_dataset.json"
    path_local = "/opt/airflow/data/raw/mobile_customers_messy_dataset.json"
    
    os.makedirs(os.path.dirname(path_local), exist_ok=True)
    response = requests.get(s3_url)
    if response.status_code == 200:
        with open(path_local, "wb") as f:
            f.write(response.content)
    else:
        raise Exception(f"Error al descargar: {response.status_code}")

def ingest_json_to_postgres():
    '''
    Ingesta el archivo JSON raw descargado en tabla raw dentro del schema bronze de PostgreSQL.
    Convierte los campos de tipo lista a JSON y agrega metadatos de ingestión.
    '''
    import pandas as pd

    json_path = "/opt/airflow/data/raw/mobile_customers_messy_dataset.json"
    df = pd.read_json(json_path)

    df['contracted_services'] = df['contracted_services'].apply(json.dumps)
    df['payment_history'] = df['payment_history'].apply(json.dumps)
    
    # Agregar metadatos de ingestión
    df['ingestion_date'] = datetime.now()
    df['source'] = 's3'

    engine = create_engine("postgresql://qversity-admin:qversity-admin@postgres:5432/qversity")
    df.to_sql("raw", engine, schema="bronze", if_exists="append", index=False)


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
    dag_id="e2e_pipeline_dag",
    default_args=default_args,
    description="Ejecuta un pipeline desde la ingesta de datos hasta la capa gold",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["e2e", "pipeline"]
) as dag:

    # Task para crear los esquemas en PostgreSQL
    create_schemas_task = PythonOperator(
        task_id="create_postgres_schemas",
        doc="Crea los esquemas bronze, silver y gold en PostgreSQL si no existen.",
        python_callable=create_postgres_schemas
    )

        
    # Tarea para descargar el JSON desde S3
    download_task = PythonOperator(
        task_id="download_data_from_s3",
        doc="Descarga un archivo JSON desde S3 y lo guarda localmente.",
        python_callable=download_json
    )

    # Tarea para ingerir el JSON descargado en PostgreSQL
    ingest_task = PythonOperator(
        task_id="ingest_local_data_to_postgres",
        doc="Ingesta el archivo JSON descargado en PostgreSQL en la tabla raw del schema bronze.",
        python_callable=ingest_json_to_postgres
    )

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

    run_gold = BashOperator(
        task_id="dbt_run_gold",
        doc="Ejecuta dbt run para transformar los datos de capa silver a gold.",
        bash_command="cd /dbt && dbt run --select gold"
    )

    test_gold = BashOperator(
        task_id="dbt_test_gold",
        doc="Ejecuta dbt test para validar la calidad de los datos transformados en la capa gold.",
        bash_command="cd /dbt && dbt test --select gold"
    )

    # Definición de la secuencia de tareas
    create_schemas_task >> download_task
    download_task >> ingest_task
    ingest_task >> run_silver
    run_silver >> test_silver
    test_silver >> run_gold
    run_gold >> test_gold
