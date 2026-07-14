from airflow.providers.ssh.operators.ssh import SSHOperator

run_dbt = SSHOperator(
    task_id="run_dbt_duckdb",
    ssh_conn_id="dbt_duckdb_droplet",
    command=(
        "cd /caminho/do/projeto && "
        "docker compose run --rm dbt run --target dev_duckdb && "
        "docker compose run --rm dbt test --target dev_duckdb"
    ),
    cmd_timeout=600,
)
