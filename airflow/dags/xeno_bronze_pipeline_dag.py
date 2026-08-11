"""
DAG: xeno_bronze_pipeline

Fluxo:
  1. ingest_to_s3        -> baixa metadados da API Xeno-canto e sobe
                             bird-sp-<especie>.json para s3://xeno-canto-s3/bronze/
  2. load_bronze         -> dispara (via SSH) dbt run-operation load_bronze_recordings,
                             que executa o COPY INTO no Snowflake (S3 -> RAW.SRC_RECORDINGS)
  3. dbt_run_staging     -> dispara (via SSH) dbt run --select staging
                             cria/atualiza a view STAGING.STG_RECORDINGS
  4. dbt_test            -> dispara (via SSH) dbt test

Pré-requisitos no Airflow:
  - Connection SSH configurada apontando para o droplet do dbt
    (ajuste SSH_CONN_ID abaixo para o conn_id real já usado no seu projeto).
  - O container airflow-worker precisa ter o volume da chave SSH montado
    (mesma correção já aplicada anteriormente no docker-compose.yml).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

# ------------------------------------------------------------------
# Configurações
# ------------------------------------------------------------------
SSH_CONN_ID = "dbt_remote"  # ajuste para o conn_id real já configurado
DBT_WORKDIR = "/root/dbt"            # ajuste para o path do projeto dbt no droplet
DBT_RUN_CMD = "docker compose run --rm dbt"

default_args = {
    "owner": "flavio",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


# def ingest_species_to_s3(**context):
#     """
#     Chama o pipeline de ingest já existente (config.py / ingest.py)
#     para buscar os dados da espécie-alvo no Xeno-canto e subir o JSON
#     resultante para s3://xeno-canto-s3/bronze/birds-sp-<especie>.json.

#     Ajuste o import abaixo para o caminho real do seu módulo/pacote
#     (o pacote com config.py, ingest.py, etc. precisa estar instalado
#     ou acessível no PYTHONPATH do container airflow-worker).
#     """
#     from pipeline.ingest import fetch_species_and_upload_to_s3  # noqa: E402
#     from pipeline.config import TARGET_SPECIES, S3_BUCKET, S3_BRONZE_PREFIX  # noqa: E402

#     result = fetch_species_and_upload_to_s3(
#         species=TARGET_SPECIES,
#         bucket=S3_BUCKET,
#         prefix=S3_BRONZE_PREFIX,
#     )
#     context["ti"].xcom_push(key="uploaded_file", value=result)
#     return result


with DAG(
    dag_id="xeno_bronze_pipeline",
    description="Ingest Xeno-canto -> S3 -> Bronze (Snowflake) -> Staging dbt -> Tests",
    default_args=default_args,
    schedule=None,  # dispare manualmente ou ajuste para @daily etc.
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["xeno-canto", "bronze", "dbt", "snowflake"],
) as dag:

    # ingest_to_s3 = PythonOperator(
    #     task_id="ingest_to_s3",
    #     python_callable=ingest_species_to_s3,
    # )

    load_bronze = SSHOperator(
        task_id="load_bronze",
        ssh_conn_id=SSH_CONN_ID,
        command=f"cd {DBT_WORKDIR} && {DBT_RUN_CMD} run-operation load_bronze_recordings",
        cmd_timeout=300,
    )

    dbt_run_staging = SSHOperator(
        task_id="dbt_run_staging",
        ssh_conn_id=SSH_CONN_ID,
        command=f"cd {DBT_WORKDIR} && {DBT_RUN_CMD} run --select staging",
        cmd_timeout=600,
    )

    dbt_test = SSHOperator(
        task_id="dbt_test",
        ssh_conn_id=SSH_CONN_ID,
        command=f"cd {DBT_WORKDIR} && {DBT_RUN_CMD} test",
        cmd_timeout=600,
    )

    ingest_to_s3 >> load_bronze >> dbt_run_staging >> dbt_test
