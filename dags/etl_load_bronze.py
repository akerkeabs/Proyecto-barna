import os
import pandas as pd
import requests
import s3fs
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "airflow")
SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "airflow")
BRONZE_BUCKET = "bronze"

API_BASE_URL = "https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search"
RESOURCE_IDS_BY_YEAR = {
    2022: "87a8aeda-d3eb-4ba5-bcad-b9ab0c296df5",
    2023: "5a040155-38b3-4b19-a4b0-c84a0618d363",
    2024: "8cfddcbe-3403-4a6c-8897-c13238da900e",
    2025: "796504f6-7602-41a7-82a9-d3bd47e68dee"
}


def extract_and_load_bronze(**context):
    print("1. Obteniendo datos de la API...")
    all_data_frames = []
    headers = {'User-Agent': 'Mozilla/5.0'}

    for year, resource_id in RESOURCE_IDS_BY_YEAR.items():
        params = {'resource_id': resource_id, 'limit': 32000}
        response = requests.get(API_BASE_URL, params=params, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if data['success']:
                df_year = pd.DataFrame(data['result']['records'])
                df_year['NK_Any'] = year
                all_data_frames.append(df_year)
                print(f"Datos obtenidos para {year}. Filas: {len(df_year)}")

    if not all_data_frames:
        raise Exception("No se pudieron obtener datos.")

    df_raw = pd.concat(all_data_frames, ignore_index=True)

    # Conexión a MinIO y creación de Bucket si no existe
    fs = s3fs.S3FileSystem(
        key=ACCESS_KEY, secret=SECRET_KEY, client_kwargs={"endpoint_url": MINIO_ENDPOINT}
    )
    if not fs.exists(BRONZE_BUCKET):
        print(f"Creando bucket '{BRONZE_BUCKET}'...")
        fs.mkdir(BRONZE_BUCKET)

    # Guardado en Parquet
    ds = context['ds']
    file_path = f"s3://{BRONZE_BUCKET}/accidentes_raw_{ds}.parquet"

    storage_options = {"key": ACCESS_KEY, "secret": SECRET_KEY, "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}}
    df_raw.to_parquet(file_path, storage_options=storage_options, index=False, engine='pyarrow')

    print(f"Datos guardados en Bronze: {file_path}")
    return file_path


# =========================
# DEFINICIÓN DEL DAG
# =========================
with DAG(
        dag_id='01_ingesta_api_bronze',
        schedule='@daily',
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['bronze', 'api'],
) as dag:
    tarea_bronze = PythonOperator(
        task_id='fetch_api_to_bronze',
        python_callable=extract_and_load_bronze,
    )

    disparar_silver = TriggerDagRunOperator(
        task_id="trigger_silver_cleaning",
        trigger_dag_id="02_limpieza_y_carga_silver",
        conf={"bronze_file_path": "{{ ti.xcom_pull(task_ids='fetch_api_to_bronze') }}"}
    )

    tarea_bronze >> disparar_silver