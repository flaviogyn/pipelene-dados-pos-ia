from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

project = ProjectConfig(
    dbt_project_path="/usr/local/airflow/include/laboratorio_dbt",
)

profile = ProfileConfig(
    profile_name="laboratorio_dbt",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_dev", # usa a Connection criada manualmente no Airflow
        profile_args={
            "database": "LAB_PIPELINE",
            "schema": "CORE",
            "warehouse": "LAB_WH_DBT",
            "role": "DBT_DEV",
        },
    ),
)

dag = DbtDag(
    dag_id="laboratorio_dbt",
    project_config=project,
    profile_config=profile,
    start_date=datetime(2025, 1, 1),
    schedule=None,  # ou "@daily"
)
