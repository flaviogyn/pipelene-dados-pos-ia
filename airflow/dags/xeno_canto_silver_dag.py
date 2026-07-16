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
import librosa
import pandas as pd

logger = logging.getLogger(__name__)

def extract_audio_features(signal, sr) -> dict:
    """
    Extrai atributos sonoros simples de um arquivo de áudio.
    Esses atributos serão usados na camada ouro ou para treinamento.
    Baseado no código do professor Otávio.
    """
    import numpy as np

    duration_seconds = librosa.get_duration(
        y=signal,
        sr=sr
    )

    # Energia média do sinal.
    # Valores maiores indicam sinais mais intensos.
    energy_mean = float(np.mean(signal ** 2))

    # RMS representa a magnitude média do sinal.
    # É uma medida comum de intensidade sonora.
    rms = librosa.feature.rms(y=signal)
    rms_mean = float(np.mean(rms))
    rms_std = float(np.std(rms))

    # Zero Crossing Rate mede quantas vezes o sinal cruza o zero.
    # Sons mais agudos ou ruidosos tendem a ter valores maiores.
    zcr = librosa.feature.zero_crossing_rate(y=signal)
    zcr_mean = float(np.mean(zcr))
    zcr_std = float(np.std(zcr))

    # Spectral Centroid indica onde está o "centro de massa" do espectro.
    # Em geral, sons mais agudos têm centroides maiores.
    spectral_centroid = librosa.feature.spectral_centroid(
        y=signal,
        sr=sr
    )
    spectral_centroid_mean = float(np.mean(spectral_centroid))

    # Spectral Bandwidth mede a dispersão das frequências ao redor do centroide.
    spectral_bandwidth = librosa.feature.spectral_bandwidth(
        y=signal,
        sr=sr
    )
    spectral_bandwidth_mean = float(np.mean(spectral_bandwidth))

    # MFCCs são atributos muito usados em tarefas de áudio e fala.
    # Aqui extraímos 5 coeficientes médios para manter a demonstração simples.
    mfcc = librosa.feature.mfcc(
        y=signal,
        sr=sr,
        n_mfcc=20
    )

    mfcc_means = np.mean(mfcc, axis=1)
    mfcc_stds = np.std(mfcc, axis=1)

    resultado = {
        "duration_seconds": float(duration_seconds),
        "energy_mean": energy_mean,
        "rms_mean": rms_mean,
        "rms_std": rms_std,
        "zcr_mean": zcr_mean,
        "zcr_std": zcr_std,
        "spectral_centroid_mean": spectral_centroid_mean,
        "spectral_bandwidth_mean": spectral_bandwidth_mean
    }

    resultado.update({
        f"mfcc_{i+1}_mean": float(valor)
        for i, valor in enumerate(mfcc_means)
    })

    resultado.update({
        f"mfcc_{i+1}_std": float(valor)
        for i, valor in enumerate(mfcc_stds)
    })

    return resultado

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
    def obter_arquivos_bronze():
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
            logger.info("Falha ao carregar a variável de bucket")
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
        s3_bucket = Variable.get('S3_Bucket', default=None)
        if not s3_bucket:
            logger.info("Falha ao carregar a variável de bucket")
            raise AirflowConfigException(
                "A variável 's3_bucket' não foi cadastrada no Airflow."
            )

        logger.info("Processando %s", gravacao["id"])

        # Criando o Hook S3 para ser usado nas duas próximas ações
        hook = S3Hook(aws_conn_id='aws_conn')

        caminho_audio = hook.download_file(
            key=gravacao["audio"],
            bucket_name=s3_bucket,
            local_path="/tmp"
        )

        signal, sr = librosa.load(caminho_audio, sr=None)
        audio_features = extract_audio_features(signal, sr)

        import json
        metadados = json.loads(hook.read_key(key=gravacao["metadata"], bucket_name=s3_bucket))

        # Criação do dataframe para este arquivo de áudio
        colunas = ["id", "gen", "sp", "ssp", "lat", "lon", "alt", "grp"]
        registro = {chave: metadados[chave] for chave in colunas}
        registro |= audio_features
        df = pd.DataFrame([registro])

        # Padronização dos dados
        colunas_float = ["lat", "lon", "alt"]
        df[colunas_float] = df[colunas_float].apply(pd.to_numeric, errors="coerce")
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")


        # Vamos gerar aqui o arquivo parquet para ser salvo na camada
        # prata. O arquivo é gerado em memória e então carregado
        # no Bucket via Hook.
        from io import BytesIO

        buffer = BytesIO()

        df.to_parquet(
            buffer,
            engine="pyarrow",
            index=False
        )

        buffer.seek(0)

        hook.load_bytes(
            bytes_data=buffer.getvalue(),
            key=f"silver/{gravacao["id"]}.parquet",
            bucket_name=s3_bucket,
            replace=True
        )

    # Aqui está o processo de upload de arquivos no Bucket S3
    lista_gravacoes = obter_arquivos_bronze()

    # expand vai gerar uma task para cada arquivo tratado, aumentando
    # a velocidade de tratamento.
    processar_gravacao.expand(gravacao=lista_gravacoes)

# Instanciando o DAG
ai_xeno_canto_silver()