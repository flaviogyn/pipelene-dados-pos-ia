from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "Wallace Camargo",
    "depends_on_past": False,
}

with DAG(
    dag_id="dbt-snowflake-process",
    default_args=default_args,
    start_date=datetime(2024, 10, 31), 
    schedule_interval="@hourly",
    catchup=False,
    tags=["dbt", "snowflake"],
) as dag:

    run_transformn = DockerOperator(
        task_id="run_transformn",
        image="dbt-snowflake", 
        container_name="transform",
        api_version="auto",
        auto_remove=True,
        command="dbt run --models transform --profiles-dir .", 
        docker_url="tcp://docker-proxy:2375",
        network_mode="airflow_default",
        mount_tmp_dir=False,
    )
    
    run_analysis = DockerOperator(
        task_id="run_analysis",
        image="dbt-snowflake",
        container_name="analysis",  
        api_version="auto",
        auto_remove=True,
        command="dbt run --models analysis --profiles-dir .",  
        docker_url="tcp://docker-proxy:2375",
        network_mode="airflow_default",
        mount_tmp_dir=False, 
    )

    run_transformn >> run_analysis
