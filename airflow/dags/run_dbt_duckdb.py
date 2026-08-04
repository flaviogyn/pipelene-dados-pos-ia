from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

DBT_PROJECT_DIR = "/root/dbt"
DBT_TARGET = os.getenv("DBT_TARGET", "dev_postgres")

with DAG(
    dag_id="run_dbt_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt"]
) as dag:
    run_dbt = SSHOperator(
        task_id="run_dbt",
        ssh_conn_id="dbt_remote",
        command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"docker compose run --rm dbt run --target {DBT_TARGET} && "
            f"docker compose run --rm dbt test --target {DBT_TARGET}"
        ),
        cmd_timeout=600,
    )
