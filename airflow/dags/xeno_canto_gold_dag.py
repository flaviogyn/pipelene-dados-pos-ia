"""
DAG: xeno_silver_pipeline

Fluxo (bronze removida -- metadados e features vêm juntos no parquet
da silver, produzido por um job externo de feature extraction):

  1. load_silver      -> dispara (via SSH) dbt run-operation load_silver_features,
                          que executa o COPY INTO no Snowflake
                          (S3 parquet -> RAW.SRC_RECORDING_FEATURES)
  2. dbt_run_staging  -> dispara (via SSH) dbt run --select staging
                          cria/atualiza a view STAGING.STG_RECORDING_FEATURES
  3. dbt_run_marts    -> dispara (via SSH) dbt run --select marts
                          cria/atualiza GOLD.DIM_SPECIES e GOLD.FCT_RECORDINGS
                          (lidas pelo Metabase)
  4. dbt_test         -> dispara (via SSH) dbt test

Pré-requisitos no Airflow:
  - Connection SSH configurada apontando para o droplet do dbt
    (ajuste SSH_CONN_ID abaixo para o conn_id real já usado no seu projeto).
  - O container airflow-worker precisa ter o volume da chave SSH montado.
  - Esta DAG pressupõe que o job externo de feature extraction (librosa)
    já escreveu os arquivos parquet em s3://xeno-canto-s3/silver/ antes
    dela rodar. Se esse job também roda no Airflow, adicione a task dele
    como primeira etapa, antes de load_silver.
"""

from datetime import datetime, timedelta

from airflow import DAG
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

with DAG(
    dag_id="ai_xeno_canto_gold",
    description="Silver (S3 parquet) -> Snowflake -> Staging dbt -> Gold -> Tests",
    default_args=default_args,
    schedule=None,  # dispare manualmente ou ajuste para @daily etc.
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["xeno-canto", "silver", "gold", "dbt", "snowflake"],
) as dag:

    load_silver = SSHOperator(
        task_id="load_silver",
        ssh_conn_id=SSH_CONN_ID,
        command=f"cd {DBT_WORKDIR} && {DBT_RUN_CMD} run-operation load_silver_features",
        cmd_timeout=300,
    )

    dbt_run_staging = SSHOperator(
        task_id="dbt_run_staging",
        ssh_conn_id=SSH_CONN_ID,
        command=f"cd {DBT_WORKDIR} && {DBT_RUN_CMD} run --select staging",
        cmd_timeout=600,
    )

    dbt_run_marts = SSHOperator(
        task_id="dbt_run_marts",
        ssh_conn_id=SSH_CONN_ID,
        command=f"cd {DBT_WORKDIR} && {DBT_RUN_CMD} run --select marts",
        cmd_timeout=600,
    )

    dbt_test = SSHOperator(
        task_id="dbt_test",
        ssh_conn_id=SSH_CONN_ID,
        command=f"cd {DBT_WORKDIR} && {DBT_RUN_CMD} test",
        cmd_timeout=600,
    )

    load_silver >> dbt_run_staging >> dbt_run_marts >> dbt_test
