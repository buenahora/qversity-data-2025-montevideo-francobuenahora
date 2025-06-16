from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
import pandas as pd
from sqlalchemy import create_engine
import json
import os
import requests

# Descargar el archivo JSON desde S3 y guardarlo localmente
def download_data_from_s3():
    S3_URL = "https://qversity-raw-public-data.s3.amazonaws.com/mobile_customers_messy_dataset.json"
    LOCAL_PATH = "/opt/airflow/data/raw/mobile_customers_messy_dataset.json"
    os.makedirs(os.path.dirname(LOCAL_PATH), exist_ok=True)
    response = requests.get(S3_URL)
    if response.status_code == 200:
        with open(LOCAL_PATH, "wb") as f:
            f.write(response.content)
        print(f"Archivo descargado y guardado en {LOCAL_PATH}")
    else:
        raise Exception(f"Fallo la descarga. Código de estado: {response.status_code}")

def ingest_local_data_to_postgres():
    # Ruta local dentro del contenedor (mapeada desde el host)
    json_path = "/opt/airflow/data/raw/mobile_customers_messy_dataset.json"

    # Leer el JSON desde archivo local
    df = pd.read_json(json_path)

    # Convertir columnas complejas a string JSON, tendria que ver si no mejor usar json.dumps o pasar todo a string
    df['contracted_services'] = df['contracted_services'].apply(json.dumps)
    df['payment_history'] = df['payment_history'].apply(json.dumps)

    # Agregar una columna con fecha de ingestion y source
    df['ingestion_date'] = datetime.now()
    df['source'] = 's3'

    # Conectar a PostgreSQL
    engine = create_engine('postgresql://qversity-admin:qversity-admin@postgres:5432/qversity')

    # Cargar en esquema bronze, tabla raw
    df.to_sql('raw', engine, schema='bronze', if_exists='append', index=False)

    print("✅ Datos cargados en bronze.raw")

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
    "ingest_data_dag",
    default_args=default_args,
    description="Carga datos desde archivo local descargado de S3",
    schedule_interval=None,  # Solo ejecución manual
    catchup=False
)

ingest_task = PythonOperator(
    task_id="ingest_local_data_to_postgres",
    python_callable=ingest_local_data_to_postgres,
    dag=dag
)

download_data_from_s3 = PythonOperator(
    task_id="download_data_from_s3",
    python_callable=download_data_from_s3,
    dag=dag
)

download_data_from_s3 >> ingest_task