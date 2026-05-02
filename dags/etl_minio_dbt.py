import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from sqlalchemy import create_engine
from datetime import datetime, timedelta

GITHUB_BASE_URL = "https://raw.githubusercontent.com/USER_GIT/Open-Data-II/main"
AÑOS_A_DESCARGAR = [2022, 2023, 2024, 2025]

# CAMBIO 1: Usar 'minio' en lugar de 'localhost'
MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")

# (Nota: Recuerda que lo ideal aquí sería usar PostgresHook en lugar de os.getenv)
POSTGRES_CREDS = f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@postgres:5432/{os.getenv('POSTGRES_DB')}"


def extract_and_load():
    print("1. Descargando CSVs de GitHub...")
    lista_dfs = []

    for año in AÑOS_A_DESCARGAR:
        url = f"{GITHUB_BASE_URL}/{año}_accidents_causa_conductor_gu_bcn_.csv"
        print(f"Descargando datos de {año}...")
        try:
            df_temp = pd.read_csv(url)
            lista_dfs.append(df_temp)
        except Exception as e:
            print(f"Error descargando el año {año}: {e}")

    df = pd.concat(lista_dfs, ignore_index=True)
    df.columns = [col.strip().lower() for col in df.columns]

    fecha_hoy = datetime.now()
    df['etl_fecha_ingesta'] = fecha_hoy
    print(f"Se agruparon {len(df)} filas en total.")

    # ---------------------------------------------------------
    # BACKUP EN MINIO (Data Lake)
    # ---------------------------------------------------------
    print("2. Guardando copia de seguridad en MinIO...")

    # CAMBIO 2: Usar las variables correctas que definiste arriba
    storage_options = {
        "key": ACCESS_KEY,
        "secret": SECRET_KEY,
        "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
    }

    nombre_archivo = f"accidentes_historico_raw_{fecha_hoy.strftime('%Y%m%d')}.parquet"
    ruta_s3 = f"s3://{BUCKET_NAME}/raw/{nombre_archivo}"

    df.to_parquet(ruta_s3, storage_options=storage_options, engine='pyarrow')
    print(f"Backup guardado en: {ruta_s3}")

    # ---------------------------------------------------------
    # CARGA EN POSTGRESQL (Para dbt)
    # ---------------------------------------------------------
    print("3. Cargando en PostgreSQL...")
    engine = create_engine(POSTGRES_CREDS)
    df.to_sql('stg_accidentes_github', engine, if_exists='replace', index=False)
    print("Carga en BD finalizada.")


# ==========================================
# 2. DEFINICIÓN DEL DAG
# ==========================================
default_args = {
    'owner': 'data_engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
        'ingesta_github_accidentes',
        default_args=default_args,
        schedule_interval='@daily',
        start_date=datetime(2023, 10, 1),
        catchup=False,
) as dag:
    tarea_ingesta = PythonOperator(
        task_id='extraer_github_respaldar_y_cargar',
        python_callable=extract_and_load,
    )

    tarea_dbt = BashOperator(
        task_id='transformar_con_dbt',
        bash_command='cd /opt/airflow/dbt_project && dbt run',
    )

    tarea_ingesta >> tarea_dbt