from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import os
import requests

# Funciones del DAG
def download_json():
    S3_URL = "https://qversity-raw-public-data.s3.amazonaws.com/mobile_customers_messy_dataset.json"
    LOCAL_PATH = "/opt/airflow/data/raw/mobile_customers_messy_dataset.json"
    
    os.makedirs(os.path.dirname(LOCAL_PATH), exist_ok=True)
    response = requests.get(S3_URL)
    if response.status_code == 200:
        with open(LOCAL_PATH, "wb") as f:
            f.write(response.content)
    else:
        raise Exception(f"❌ Error al descargar: Código {response.status_code}")

def ingest_json_to_postgres():
    import pandas as pd
    from sqlalchemy import create_engine

    json_path = "/opt/airflow/data/raw/mobile_customers_messy_dataset.json"
    df = pd.read_json(json_path)

    df['contracted_services'] = df['contracted_services'].apply(json.dumps)
    df['payment_history'] = df['payment_history'].apply(json.dumps)
    df['ingestion_date'] = datetime.now()
    df['source'] = 's3'

    engine = create_engine("postgresql://qversity-admin:qversity-admin@postgres:5432/qversity")
    df.to_sql("raw", engine, schema="bronze", if_exists="append", index=False)


# Argumentos por defecto del DAG
default_args = {
    "owner": "qversity",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Definicion del DAG
with DAG(
    dag_id="ingest_data_dag",
    default_args=default_args,
    description="Descarga un JSON desde S3 y lo ingesta en PostgreSQL (bronze)",
    schedule_interval=None,
    catchup=False,
    tags=["ingestion", "bronze"]
) as dag:

    download_task = PythonOperator(
        task_id="download_data_from_s3",
        python_callable=download_json
    )

    ingest_task = PythonOperator(
        task_id="ingest_local_data_to_postgres",
        python_callable=ingest_json_to_postgres
    )

    download_task >> ingest_task
