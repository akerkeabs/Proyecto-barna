import os
import pandas as pd
import s3fs
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

USER_GIT = os.getenv("USER_GIT")
GITHUB_BASE_URL = f"https://raw.githubusercontent.com/{USER_GIT}/Open-Data-II/main"
YEARS_TO_DOWNLOAD = [2022, 2023, 2024, 2025]

MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")

POSTGRES_CREDS = f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@postgres:5432/{os.getenv('POSTGRES_DB')}"


def extract_and_load():
    print("1. Descargando CSVs de GitHub...")
    lista_dfs = []

    for year in YEARS_TO_DOWNLOAD:
        url = f"{GITHUB_BASE_URL}/{year}_accidents_causa_conductor_gu_bcn.csv"
        print(f"Descargando datos de {year}...")
        try:
            df_temp = pd.read_csv(url)
            df_temp.columns = [str(col).strip().lower() for col in df_temp.columns]
            lista_dfs.append(df_temp)
        except Exception as e:
            print(f"Error descargando el year {year}: {e}")

    df = pd.concat(lista_dfs, ignore_index=True)
    df = df.loc[:, ~df.columns.duplicated()]

    today_date = datetime.now()
    df['etl_fecha_ingesta'] = today_date
    print(f"Se agruparon {len(df)} filas en total.")

    # ---------------------------------------------------------
    # BACKUP EN MINIO (Data Lake)
    # ---------------------------------------------------------
    print("2. Guardando copia de seguridad en MinIO...")
    fs = s3fs.S3FileSystem(
        key=ACCESS_KEY,
        secret=SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT}
    )

    # Si el bucket existe guarda, si no, lo creamos
    if not fs.exists(BUCKET_NAME):
        print(f"El bucket '{BUCKET_NAME}' no existe. Creando...")
        fs.mkdir(BUCKET_NAME)
    else:
        print(f"El bucket '{BUCKET_NAME}' ya existe. Continua")

    storage_options = {
        "key": ACCESS_KEY,
        "secret": SECRET_KEY,
        "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
    }

    file_name = f"accidentes_historico_raw_{today_date.strftime('%Y%m%d')}.parquet"
    ruta_s3 = f"s3://{BUCKET_NAME}/raw/{file_name}"

    df.to_parquet(ruta_s3, storage_options=storage_options, engine='pyarrow')
    print(f"Backup guardado en: {ruta_s3}")

    # ---------------------------------------------------------
    # CARGA EN POSTGRESQL (Para dbt)
    # ---------------------------------------------------------
    print("3. Cargando en PostgreSQL...")
    engine = create_engine(POSTGRES_CREDS)

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS stg_accidentes_github CASCADE;"))
        conn.execute(text("DROP TYPE IF EXISTS stg_accidentes_github CASCADE;"))

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
        schedule='@daily',
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