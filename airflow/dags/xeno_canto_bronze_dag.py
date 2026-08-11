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
import re
import os
import pendulum

logger = logging.getLogger(__name__)

def format_query_for_key(query: str) -> str:
    """
    Converte uma query Xeno-Canto em um nome seguro para uso de arquivo.
    Exemplo: 'sp:"Pitangus sulphuratus"' -> 'sp-pitangus-sulphuratus'
    """
    cleaned = query.replace('"', '').replace(':', '-')
    cleaned = re.sub(r'\s+', '-', cleaned)
    cleaned = re.sub(r'[^a-zA-Z0-9\-]', '', cleaned)
    return cleaned.lower()

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
    def obter_configuracao():
        """
        Obtém configurações de download do Airflow.
        Assim, pode-se parametrizar quais aves e quantos arquivos
        a DAG vai buscar no servidor da Xeno-Canto.
        """
        config = Variable.get("xeno_canto_config", deserialize_json=True)
        if not config:
            raise AirflowConfigException(
                "A variável 'xeno_canto_config' não foi cadastrada "
                "ou está vazia."
            )

        return config

    @task
    def preparar_consultas(config):
        """
        Para manter as configurações das espécies separadas e
        permitir a paralelização do processo, criou-se esta
        função. Ela retorna uma lista de configurações que o
        Dynamic Task Mapping do airflow pode usar.
        """
        return [
        {
            "especie": especie,
            "quantidade": quantidade
        } 
        for especie, quantidade in config.items()]

    @task
    def obter_lista_gravacoes(especie, quantidade):
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

        # Monta a query a partir da espécie.
        query = f'sp:"{especie}"'

        logger.info(f"Iniciando busca de gravações para {especie}.")
        # Aqui faz a requisição da lista de arquivos com
        # as gravações de audio.
        url = "https://xeno-canto.org/api/3/recordings"
        params = {"query": query, "key": xeno_key}

        response = requests.get(url, params=params)
        response.raise_for_status()
        dados = response.json()

        logger.info(f"Encontradas {dados['numRecordings']} gravações "
                    f"para {especie}.")

        # Para salvar o arquivo no S3, devemos converter
        # os dados em memória para um arquivo.
        # Fazemos isto aqui.
        arquivo = io.BytesIO(json.dumps(dados).encode("utf-8"))

        # Gera o nome da key a partir da query.
        nome_arquivo = format_query_for_key(query)

        logger.info("Salvando query no Bucket S3.")
        # Cria um Hook S3 e salva o arquivo.
        hook = S3Hook(aws_conn_id='aws_conn')
        hook.load_file_obj(
            file_obj=arquivo,
            key=f'bronze/query/{nome_arquivo}.json',
            bucket_name=s3_bucket,
            replace=True
        )

        # Retorna a lista obtida para posterior
        # processamento
        return {
            'especie': especie, 
            'quantidade': quantidade, 
            'dados': dados
        }

    @task
    def selecionar_audios(resultado):
        """
        Seleciona a quantidade de gravações definida
        para uma determinada espécie.
        """
        quantidade = resultado['quantidade']
        dados = resultado['dados']
        gravacoes = dados['recordings']    # Campo recordings definido pela
                                           # API da Xeno-Canto é um array
                                           # json de todas as gravações
                                           # encontradas.
        return gravacoes[:quantidade]

    @task
    def juntar_audios(listas_audios):
        """
        Junta as listas de gravações de todas as espécies baixadas
        em uma única lista.
        """
        todas_gravacoes = []

        for lista in listas_audios:
            todas_gravacoes.extend(lista)

        return todas_gravacoes

    @task(
        retries=5,
        retry_delay=pendulum.duration(seconds=5),
        retry_exponential_backoff=True,
        max_retry_delay=pendulum.duration(minutes=5),
        pool="xeno_canto_download"
    )
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

        logger.info(f"Tratamento da gravação de ID {gravacao["id"]}.")

        # Criando o Hook S3 para ser usado nas duas próximas ações
        hook = S3Hook(aws_conn_id='aws_conn')

        nome_original = gravacao['file-name']
        # Obtém a extensão do arquivo original.
        _, extensao = os.path.splitext(nome_original)

        if not extensao:
            raise AirflowException(
                f"Não foi possível determinar a extensão do arquivo "
                f"{nome_original}."
            )

        # Salvando o áudio no Bucket S3
        logger.info(f"Obtendo arquivo {gravacao['id']}{extensao}.")
        audio = requests.get(gravacao["file"], timeout=60)
        audio.raise_for_status()
        arquivo_audio = io.BytesIO(audio.content)

        logger.info(f"Salvando arquivo {gravacao['id']}{extensao}.")
        hook.load_file_obj(
            file_obj=arquivo_audio,
            key=f"bronze/audio/{gravacao['id']}{extensao}",
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
    # O código foi escrito para aproveitar ao máximo o 
    # Dynamic Task Mapping, descrito no link:
    # https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html
    
    # 1. Obtém a configuração da Variable.
    config = obter_configuracao()

    # 2. Transforma a configuração em argumentos
    #    para o Dynamic Task Mapping.
    consultas = preparar_consultas(config)

    # 3. Executa uma consulta Xeno-Canto para cada espécie.
    resultados = obter_lista_gravacoes.expand_kwargs(consultas)

    # 4. Seleciona a quantidade configurada para cada espécie.
    audios = selecionar_audios.expand(resultado=resultados)

    # 5. Junta as listas de todas as espécies consultadas.
    todas_gravacoes = juntar_audios(audios)

    # 6. Executa um download para cada gravação.
    baixar_audio.expand(gravacao=todas_gravacoes)

# Instanciando o DAG
ai_xeno_canto_bronze()