import os
import pandas as pd
import s3fs
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from sqlalchemy import create_engine, text


MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "airflow")
SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "airflow")
SILVER_BUCKET = "silver"

# Credenciales de la BD airflow
POSTGRES_CREDS = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"


def clean_and_load_silver(**context):
    bronze_file_path = context['dag_run'].conf.get('bronze_file_path')
    if not bronze_file_path:
        raise ValueError("No se recibió la ruta del archivo Bronze.")

    print(f"1. Leyendo datos crudos desde: {bronze_file_path}")
    storage_options = {"key": ACCESS_KEY, "secret": SECRET_KEY, "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}}
    df = pd.read_parquet(bronze_file_path, storage_options=storage_options)

    # ==========================================
    # 2. LÓGICA DE LIMPIEZA
    # ==========================================
    print("2. Iniciando limpieza de datos...")

    # Unificar la columna 'causa'
    diccionario_renombres = {
        'Descripcio_causa_mediata': 'causa',
        'cause conductor': 'causa',
        'causa conductor': 'causa',
        'Descripcio_causa': 'causa'
    }
    df.rename(columns=diccionario_renombres, inplace=True)

    # Limpieza de calidad de datos
    df = df.drop_duplicates()

    if "Nom_districte" in df.columns:
        df = df[df["Nom_districte"] != "Desconegut"]

    if "Codi_districte" in df.columns:
        df["Codi_districte"] = pd.to_numeric(df["Codi_districte"], errors='coerce')
        df = df[df["Codi_districte"] != -1]
        df = df.dropna(subset=["Codi_districte"])

    geo_columns = ["Codi_districte", "Nom_districte", "Nom_barri", "Codi_barri"]
    for col in geo_columns:
        if col in df.columns:
            df = df.dropna(subset=[col])

    if "Latitud_WGS84" in df.columns and "Longitud_WGS84" in df.columns:
        df = df.dropna(subset=["Latitud_WGS84", "Longitud_WGS84"])

    # Todas las columnas a minúsculas para dbt
    df.columns = [str(col).strip().lower() for col in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]

    df['etl_fecha_ingesta'] = datetime.now()

    # ==========================================
    # 3. BACKUP EN MINIO (Capa Silver)
    # ==========================================
    fs = s3fs.S3FileSystem(key=ACCESS_KEY, secret=SECRET_KEY, client_kwargs={"endpoint_url": MINIO_ENDPOINT})
    if not fs.exists(SILVER_BUCKET):
        print(f"3. Creando bucket '{SILVER_BUCKET}'...")
        fs.mkdir(SILVER_BUCKET)

    ds = context['ds']
    silver_file_path = f"s3://{SILVER_BUCKET}/accidentes_cleaned_{ds}.parquet"
    df.to_parquet(silver_file_path, storage_options=storage_options, index=False, engine='pyarrow')
    print(f"Backup limpio guardado en Silver: {silver_file_path}")

    # ==========================================
    # 4. CARGA EN POSTGRESQL (Para dbt)
    # ==========================================
    print("4. Cargando en PostgreSQL...")
    engine = create_engine(POSTGRES_CREDS)

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS stg_accidentes_github CASCADE;"))

    df.to_sql('stg_accidentes_github', engine, if_exists='replace', index=False)
    print("Carga en BD finalizada.")


# =========================
# DEFINICIÓN DEL DAG
# =========================
with DAG(
        dag_id='02_limpieza_y_carga_silver',
        schedule=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['silver', 'postgres', 'dbt'],
) as dag:
    tarea_silver = PythonOperator(
        task_id='clean_and_load_to_postgres',
        python_callable=clean_and_load_silver,
    )

    tarea_dbt = BashOperator(
        task_id='transformar_con_dbt',
        bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir .',
    )

    tarea_silver >> tarea_dbt