"""
## DAG Xeno-canto - Silver

Esta DAG faz o tratamento dos dados carregados no S3 
"""
from airflow.sdk import Asset, dag, task
from pendulum import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowConfigException
from airflow.sdk import Variable
import logging
import pandas as pd

logger = logging.getLogger(__name__)

# Define os parâmetros básicos do DAG, como escala e data de início
@dag(
    schedule=None,
    start_date=datetime(2026, 7, 10),
    catchup=False,
    tags=["xeno-canto"]
)
def ai_xeno_canto_silver():
    # Define tasks
    @task
    def obter_arquivos_bronze(query):
        """
        Obtém a lista das gravações salvas no Bucket S3.
        Esta lista contém:
        'id': o ID da gravação que foi atribuído pelo site da Xeno-Canto
        'audio': caminho para o arquivo de áudio da gravação
        'metadata': caminho para o arquivo de metadados da gravação
        """
        # Aqui obtemos as variáveis salvas na interface
        # do Airflow. Isto permite flexibilidade.
        s3_bucket = Variable.get('S3_Bucket', default=None)

        # Verifica se as chaves foram inseridas.
        # Na falha, o Airflow vai catalogar esta DAG como
        # 'Failed'.
        if not s3_bucket:
            raise AirflowConfigException(
                "A variável 's3_bucket' não foi cadastrada no Airflow."
            )

        # Cria um Hook S3 para leitura dos dados salvos.
        hook = S3Hook(aws_conn_id='aws_conn')
        arquivos_audio = (hook.list_keys(bucket_name=s3_bucket, prefix="bronze/audio/")
                          or [])
        arquivos_metadata = (hook.list_keys(bucket_name=s3_bucket, prefix="bronze/metadata/")
                          or [])
        
        logger.info("Encontrados %d áudios e %d metadados.", len(arquivos_audio), len(arquivos_metadata))

        # Aqui é gerada a lista de saída da task. Primeiro obtém-se a lista de arquivos
        # vinculados ao ID da gravação.
        indice_metadata = {}

        from pathlib import PurePosixPath
        for arquivo in arquivos_metadata:
            nome = PurePosixPath(arquivo).stem
            indice_metadata[nome] = arquivo

        # Agora cria-se uma lista de dicionários contendo o ID, arquivo de áudio e arquivo
        # de metadados.
        gravacoes = []

        for audio in arquivos_audio:
            nome = PurePosixPath(audio).stem
            metadata = indice_metadata.get(nome)

            if metadata is None:
                logger.warning("Metadata não encontrado para %s",audio)
                continue

            gravacoes.append(
                {
                    "id": nome,
                    "audio": audio,
                    "metadata": metadata,
                }
            )

        # Retorna a lista obtida para posterior
        # processamento
        return gravacoes
    
    @task
    def processar_gravacao(gravacao):
        """
        Esta task processa uma gravação, gerando um parquet na camada silver
        do Bucket S3.
        """
        logger.info("Processando %s", gravacao["id"])
        s3_bucket = Variable.get('S3_Bucket', default=None)
        if not s3_bucket:
            raise AirflowConfigException(
                "A variável 's3_bucket' não foi cadastrada no Airflow."
            )

        logger.info(f"Tratamento da gravação {gravacao["id"]}.")

        # Criando o Hook S3 para ser usado nas duas próximas ações
        hook = S3Hook(aws_conn_id='aws_conn')


    # Aqui está o processo de upload de arquivos no Bucket S3
    lista_gravacoes = obter_arquivos_bronze()

    # expand vai gerar uma task para cada arquivo tratado, aumentando
    # a velocidade de tratamento.
    processar_gravacao.expand(gravacao=lista_gravacoes)

# Instanciando o DAG
ai_xeno_canto_silver()