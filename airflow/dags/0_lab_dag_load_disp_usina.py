from __future__ import annotations

import os
import re
import requests
import pandas as pd
from datetime import datetime

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# -------------------------
# Configurações
# -------------------------
CSV_URL = (
    "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/disponibilidade_usina_ho/"
    "DISPONIBILIDADE_USINA_2025_08.csv"
)
SNOWFLAKE_CONN_ID = "snowflake_dev"
# -------------------------

@dag(
    dag_id='load_disponibilidade_usina',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['snowflake', 'csv', 'load'],
)
def load_disponibilidade_usina_dag():
    table_name = 'DISPONIBILIDADE_USINA_2025_08_AIRFLOW'
    schema = 'STAGING'
    stage_name = 'TEMP_STAGE_USINA'
    local_csv_path = f'/tmp/{table_name}.csv'

    @task
    def download_csv() -> str:
        response = requests.get(CSV_URL)
        response.raise_for_status()
        with open(local_csv_path, 'wb') as f:
            f.write(response.content)
        return local_csv_path

    @task
    def create_table(csv_path: str):
        df = pd.read_csv(csv_path, nrows=1, sep=';')
        columns = ', '.join([f'"{col}" VARCHAR' for col in df.columns])
        create_table_sql = f'CREATE OR REPLACE TABLE {schema}.{table_name} ({columns})'
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        cursor.close()
        conn.close()

    @task
    def create_stage_and_upload(csv_path: str):
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'CREATE OR REPLACE STAGE {schema}.{stage_name}')
        # Comando PUT para upload do arquivo local para o stage
        put_sql = f"PUT file://{csv_path} @{schema}.{stage_name}/{os.path.basename(csv_path)} OVERWRITE = TRUE"
        cursor.execute(put_sql)
        cursor.close()
        conn.close()
        return os.path.basename(csv_path)

    @task
    def copy_into_table(file_name: str):
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        copy_sql = f'''
            COPY INTO {schema}.{table_name}
            FROM @{schema}.{stage_name}/{file_name}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' FIELD_DELIMITER=';' SKIP_HEADER=1)
            PURGE = TRUE
        '''
        print(copy_sql)
        cursor.execute(copy_sql)
        cursor.close()
        conn.close()
        print(f"Dados carregados na tabela {schema}.{table_name} com sucesso via COPY!")

    csv_path = download_csv()
    file_name = create_stage_and_upload(csv_path)
    create_table(csv_path) >> copy_into_table(file_name)
    
dag = load_disponibilidade_usina_dag()
