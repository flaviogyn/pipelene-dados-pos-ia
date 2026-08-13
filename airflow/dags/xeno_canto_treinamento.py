"""
## DAG Xeno-canto - Treinamento

Esta DAG faz Treinamento de Modelos de IA 
"""
from airflow.sdk import Asset, dag, task
from pendulum import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowConfigException
from airflow.sdk import Variable
import io
import json
import requests
import logging
import os
import pendulum
import pandas as pd

logger = logging.getLogger(__name__)

# Define os parâmetros básicos do DAG, como escala e data de início
@dag(
    schedule=None,
    start_date=datetime(2026, 7, 10),
    catchup=False,
    tags=["xeno-canto"]
)
def ai_xeno_canto_treinamento():
    # Define tasks
    @task
    def carregar_dados():
        s3_bucket = Variable.get('S3_Bucket', default=None)

        # Verifica se as chaves foram inseridas.
        # Na falha, o Airflow vai catalogar esta DAG como
        # 'Failed'.
        if not s3_bucket:
            raise AirflowConfigException(
                "A variável 's3_bucket' não foi cadastrada no Airflow."
            )

        logger.info("Carregando arquivo do Bucket S3.")
        # Cria um Hook S3 e salva o arquivo.
        hook = S3Hook(aws_conn_id='aws_conn')
        dados = hook.download_file(
            key='/gold/fct_recordings.parquet',
            bucket_name=s3_bucket,
            local_path="/tmp"
        )

        df = pd.read_parquet(dados)

        # Retorna a lista obtida para posterior
        # processamento
        return df

    @task
    def treinar_modelo_biblioteca(dados):
        return 1

    @task
    def treinar_modelo_hardcode(dados):
        return 1

    @task
    def salvar_resultados(resultado1, resultado2):
        s3_bucket = Variable.get('S3_Bucket', default=None)
        
        # Verifica se as chaves foram inseridas.
        # Na falha, o Airflow vai catalogar esta DAG como
        # 'Failed'.
        if not s3_bucket:
            raise AirflowConfigException(
                "A variável 's3_bucket' não foi cadastrada no Airflow."
            )

        logger.info("Salvando resultados no Bucket S3.")
        # Cria um Hook S3 e salva o arquivo.
        hook = S3Hook(aws_conn_id='aws_conn')
        hook.load_file_obj(
            file_obj=arquivo,
            key=f'treinamentos/indefinido.txt',
            bucket_name=s3_bucket,
            replace=True
        )

    # Aqui está o processo de upload de arquivos no Bucket S3
    # O código foi escrito para aproveitar ao máximo o 
    # Dynamic Task Mapping, descrito no link:
    # https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html
    
    dados = carregar_dados()

    resultado1 = treinar_modelo_biblioteca(dados)

    resultado2 = treinar_modelo_hardcode(dados)

    salvar_resultados(resultado1, resultado2)

# Instanciando o DAG
ai_xeno_canto_treinamento()