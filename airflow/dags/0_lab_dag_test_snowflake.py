from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from datetime import datetime

SNOWFLAKE_CONN_ID = 'snowflake_dev'

def test_snowflake_connection():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT CURRENT_VERSION()')
    version = cursor.fetchone()
    print(f"Conexão bem-sucedida! Versão do Snowflake: {version[0]}")
    cursor.close()
    conn.close()

with DAG(
    dag_id='test_snowflake_connection',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['snowflake', 'test'],
) as dag:
    test_conn = PythonOperator(
        task_id='test_snowflake_connection',
        python_callable=test_snowflake_connection
    )
