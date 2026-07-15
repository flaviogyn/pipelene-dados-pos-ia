"""
## DAG Xeno-canto - Bronze

Esta DAG faz download de dados da base do Xeno-Canto para o S3 
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

logger = logging.getLogger(__name__)

# Define os parâmetros básicos do DAG, como escala e data de início
@dag(
    schedule=None,
    start_date=datetime(2026, 7, 10),
    catchup=False,
    tags=["xeno-canto"]
)
def ai_xeno_canto_bronze():
    # Define tasks
    @task
    def obter_lista_gravacoes(query):
        """
        Obtém a lista de arquivos de áudio desejada.
        A lista é obtida através da API da Xeno-Canto.
        """
        # Aqui obtemos as variáveis salvas na interface
        # do Airflow. Isto permite flexibilidade e,
        # no caso da chave da Xeno-Canto, segurança.
        xeno_key = Variable.get('XenoKey', default=None)
        s3_bucket = Variable.get('S3_Bucket', default=None)

        # Verifica se as chaves foram inseridas.
        # Na falha, o Airflow vai catalogar esta DAG como
        # 'Failed'.
        if not xeno_key:
            raise AirflowConfigException(
                "A variável 'XenoKey' não foi cadastrada no Airflow."
            )
        if not s3_bucket:
            raise AirflowConfigException(
                "A variável 's3_bucket' não foi cadastrada no Airflow."
            )

        logger.info("Iniciando download das gravações.")
        # Aqui faz a requisição da lista de arquivos com
        # as gravações de audio.
        url = "https://xeno-canto.org/api/3/recordings"
        params = {
            "query": query,
            "key": xeno_key
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        dados = response.json()

        logger.info(f"Encontradas {dados['numRecordings']} gravações.")

        # Para salvar o arquivo no S3, devemos converter
        # os dados em memória para um arquivo.
        # Fazemos isto aqui.
        arquivo = io.BytesIO(
            json.dumps(dados).encode("utf-8")
        )

        logger.info("Salvando query no Bucket S3.")
        # Cria um Hook S3 e salva o arquivo.
        hook = S3Hook(aws_conn_id='aws_conn')
        hook.load_file_obj(
            file_obj=arquivo,
            key='bronze/birds.json',
            bucket_name=s3_bucket,
            replace=True
        )

        # Retorna a lista obtida para posterior
        # processamento
        return dados
    
    @task
    def selecionar_audio(lista_gravacoes):
        """
        Esta função retorna um arquivo da lista para
        download. É uma função de teste.
        """
        return lista_gravacoes["recordings"][0]

    @task
    def baixar_audio(gravacao):
        """
        Faz o download de um arquivo de áudio selecionado.
        Além disto, salva o json deste arquivo no Bucket S3.
        """
        s3_bucket = Variable.get('S3_Bucket', default=None)
        if not s3_bucket:
            raise AirflowConfigException(
                "A variável 's3_bucket' não foi cadastrada no Airflow."
            )

        logger.info(f"Tratamento da gravação {gravacao["id"]}.")

        # Criando o Hook S3 para ser usado nas duas próximas ações
        hook = S3Hook(aws_conn_id='aws_conn')

        logger.info(f"Obtendo arquivo {gravacao["file"]}.")
        # Salvando o áudio no Bucket S3
        audio = requests.get(gravacao["file"])
        audio.raise_for_status()
        arquivo_audio = io.BytesIO(audio.content)

        logger.info(f"Salvando arquivo {gravacao['id']}.mp3.")
        hook.load_file_obj(
            file_obj=arquivo_audio,
            key=f"bronze/audio/{gravacao['id']}.mp3",
            bucket_name=s3_bucket,
            replace=True
        )

        # Salvando o json com as informações do arquivo
        arquivo_json = io.BytesIO(
            json.dumps(gravacao).encode("utf-8")
        )
        logger.info(f"Salvando arquivo {gravacao['id']}.json.")
        hook.load_file_obj(
            file_obj=arquivo_json,
            key=f"bronze/metadata/{gravacao['id']}.json",
            bucket_name=s3_bucket,
            replace=True
        )

    # Aqui está o processo de upload de arquivos no Bucket S3
    lista_gravacoes = obter_lista_gravacoes('sp:"Pitangus sulphuratus"')
    audio = selecionar_audio(lista_gravacoes)
    baixar_audio(audio)

# Instanciando o DAG
ai_xeno_canto_bronze()