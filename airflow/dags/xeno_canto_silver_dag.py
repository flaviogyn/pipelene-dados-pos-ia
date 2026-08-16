"""
## DAG Xeno-canto - Silver

Esta DAG faz o tratamento dos dados carregados no S3.
"""

from airflow.sdk import dag, task, Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowConfigException, AirflowException

from pendulum import datetime
import pendulum

import logging
import librosa
import pandas as pd
import json
import os

from pathlib import PurePosixPath
from io import BytesIO

logger = logging.getLogger(__name__)

# ================================================================
# Extração das características de áudio
# ================================================================
def extract_audio_features(signal, sr) -> dict:
    """
    Extrai atributos sonoros de um arquivo de áudio.

    Esses atributos serão utilizados posteriormente
    na camada Gold e/ou no treinamento do modelo.
    """
    import numpy as np

    # Duração
    duration_seconds = librosa.get_duration(y=signal, sr=sr)

    # Energia média
    energy_mean = float(np.mean(signal ** 2))

    # RMS
    rms = librosa.feature.rms(y=signal)
    rms_mean = float(np.mean(rms))
    rms_std = float(np.std(rms))

    # Zero Crossing Rate
    zcr = librosa.feature.zero_crossing_rate(y=signal)
    zcr_mean = float(np.mean(zcr))
    zcr_std = float(np.std(zcr))

    # Spectral Centroid
    spectral_centroid = librosa.feature.spectral_centroid(y=signal, sr=sr)
    spectral_centroid_mean = float(np.mean(spectral_centroid))

    # Spectral Bandwidth
    spectral_bandwidth = librosa.feature.spectral_bandwidth(y=signal, sr=sr)
    spectral_bandwidth_mean = float(np.mean(spectral_bandwidth))

    # MFCC
    mfcc = librosa.feature.mfcc(y=signal, sr=sr, n_mfcc=20)
    mfcc_means = np.mean(mfcc, axis=1)
    mfcc_stds = np.std(mfcc, axis=1)

    # ------------------------------------------------------------
    # Resultado
    # ------------------------------------------------------------
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

    # Médias dos 20 MFCCs
    resultado.update({
        f"mfcc_{i + 1}_mean": float(valor)
        for i, valor in enumerate(mfcc_means)
    })

    # Desvios-padrão dos 20 MFCCs
    resultado.update({
        f"mfcc_{i + 1}_std": float(valor)
        for i, valor in enumerate(mfcc_stds)
    })

    return resultado

# ================================================================
# DAG
# ================================================================
@dag(
    schedule=None,
    start_date=datetime(2026, 7, 10),
    catchup=False,
    tags=["xeno-canto"]
)
def ai_xeno_canto_silver():

    @task
    def obter_arquivos_bronze():
        """
        Obtém os arquivos de áudio e seus respectivos metadados
        presentes na camada Bronze.

        Os arquivos são divididos em lotes para evitar que uma
        quantidade muito grande de itens seja enviada através
        do XCom e utilizada pelo Dynamic Task Mapping.
        """

        s3_bucket = Variable.get('S3_Bucket', default=None)

        # Verifica se a chave foram inseridas.
        # Na falha, o Airflow vai catalogar esta DAG como
        # 'Failed'.
        if not s3_bucket:
            logger.error("Falha ao carregar a variável de bucket.")
            raise AirflowConfigException("A variável 'S3_Bucket' não foi cadastrada no Airflow.")

        # Cria um Hook S3 para leitura dos dados salvos.
        hook = S3Hook(aws_conn_id='aws_conn')

        arquivos_audio = (hook.list_keys(bucket_name=s3_bucket, prefix="bronze/audio/") 
                          or [])
        arquivos_metadata = (hook.list_keys(bucket_name=s3_bucket, prefix="bronze/metadata/")
            or [])

        logger.info(
            "Encontrados %d áudios e %d metadados.",
            len(arquivos_audio),
            len(arquivos_metadata)
        )

        # Cria índice dos metadados pelo ID
        indice_metadata = {}
        for arquivo in arquivos_metadata:
            nome = PurePosixPath(arquivo).stem
            indice_metadata[nome] = arquivo

        # Cria lista de gravações
        gravacoes = []
        for audio in arquivos_audio:
            nome = PurePosixPath(audio).stem
            metadata = indice_metadata.get(nome)

            if metadata is None:
                logger.warning(
                    "Metadata não encontrado para %s",
                    audio
                )

                continue

            gravacoes.append({
                "id": nome,
                "audio": audio,
                "metadata": metadata
            })

        logger.info(
            "Total de gravações válidas encontradas: %d",
            len(gravacoes)
        )

        # Divide as gravações em lotes, pois o airflow possui uma limitação
        # de tamanho de listas (padrão é 1024)
        tamanho_lote = 10
        lotes = [
            gravacoes[i:i + tamanho_lote]
            for i in range(
                0,
                len(gravacoes),
                tamanho_lote
            )
        ]

        logger.info(
            "Foram criados %d lotes.",
            len(lotes)
        )

        return lotes

    def processar_uma_gravacao(gravacao, hook, s3_bucket):
        """
        Processa uma única gravação.

        Esta função é executada dentro do lote e possui seu
        próprio mecanismo de retry.

        Retorna True em caso de sucesso e False caso a gravação
        não possa ser processada.
        """

        id_gravacao = gravacao["id"]
        max_tentativas = 3

        for tentativa in range(1, max_tentativas + 1):
            caminho_audio = None
            logger.info("------------------------------------------")
            logger.info("Processando gravação %s.", id_gravacao)
            logger.info("Tentativa %d de %d.", tentativa, max_tentativas)
            logger.info("Arquivo de áudio: %s", gravacao["audio"])
            logger.info("Arquivo de metadata: %s", gravacao["metadata"])

            try:
                caminho_audio = hook.download_file(
                    key=gravacao["audio"],
                    bucket_name=s3_bucket,
                    local_path="/tmp"
                )

                logger.info("Áudio baixado para %s.", caminho_audio)

                signal, sr = librosa.load(caminho_audio, sr=None)

                if signal is None or len(signal) == 0:
                    raise ValueError("O arquivo não contém amostras de áudio.")

                logger.info(
                    "Áudio %s carregado. "
                    "Sample rate: %d | Amostras: %d",
                    id_gravacao,
                    sr,
                    len(signal)
                )

                audio_features = extract_audio_features(signal, sr)

                logger.info("Características extraídas para %s.", id_gravacao)

                metadados = json.loads(
                    hook.read_key(
                        key=gravacao["metadata"],
                        bucket_name=s3_bucket
                    )
                )

                # ------------------------------------------------
                # Seleção de colunas
                # ------------------------------------------------
                colunas = ["id", "gen", "sp", "ssp", "en", "status", "cnt", "loc", "q", "url", "date", "type", "lat", "lon", "alt", "grp"]
                registro = {
                    chave: metadados.get(chave)
                    for chave in colunas
                }

                registro |= audio_features
                df = pd.DataFrame([registro])

                # ------------------------------------------------
                # Padronização dos dados
                # ------------------------------------------------

                colunas_float = [
                    "lat",
                    "lon",
                    "alt"
                ]

                df[colunas_float] = (
                    df[colunas_float]
                    .apply(
                        pd.to_numeric,
                        errors="coerce"
                    )
                )

                df["id"] = (
                    pd.to_numeric(
                        df["id"],
                        errors="coerce"
                    )
                    .astype("Int64")
                )

                # ------------------------------------------------
                # Validação da data
                # ------------------------------------------------

                df["date"] = pd.to_datetime(
                    df["date"],
                    errors="coerce"
                )

                # Datas inválidas são convertidas para NaT.
                # Nesses casos, utiliza-se a data atual.
                df["date"] = df["date"].fillna(
                    pd.Timestamp.now().normalize()
                )

                # Mantém a data no formato YYYY-MM-DD.
                df["date"] = df["date"].dt.strftime(
                    "%Y-%m-%d"
                )

                # ------------------------------------------------
                # Geração do Parquet
                # ------------------------------------------------
                buffer = BytesIO()

                df.to_parquet(
                    buffer,
                    engine="pyarrow",
                    index=False
                )

                buffer.seek(0)

                # ------------------------------------------------
                # Upload para Silver
                # ------------------------------------------------
                hook.load_bytes(
                    bytes_data=buffer.getvalue(),
                    key=(
                        f"silver/"
                        f"{id_gravacao}.parquet"
                    ),
                    bucket_name=s3_bucket,
                    replace=True
                )

                logger.info(
                    "Gravação %s processada com sucesso.",
                    id_gravacao
                )

                return True

            # Houve um erro ao processar o arquivo
            # Trata-se os erros aqui para continuar o fluxo.
            except Exception as erro:

                logger.exception(
                    "Falha na tentativa %d para a gravação %s: %s",
                    tentativa,
                    id_gravacao,
                    erro
                )

                # Remove arquivo temporário antes de tentar
                # novamente.
                if (caminho_audio is not None and os.path.exists(caminho_audio)):
                    try:
                        os.remove(caminho_audio)

                        logger.info("Arquivo temporário removido: %s", caminho_audio)

                    except Exception as erro_remocao:
                        logger.warning(
                            "Não foi possível remover o arquivo "
                            "temporário %s: %s",
                            caminho_audio,
                            erro_remocao
                        )

                # Se ainda houver tentativas, aguarda antes
                # de tentar novamente.
                if tentativa < max_tentativas:
                    segundos = 5 * tentativa
                    logger.info(
                        "Aguardando %d segundos antes da "
                        "próxima tentativa.",
                        segundos
                    )

                    import time
                    time.sleep(segundos)

                # Todas as tentativas falharam.
                else:
                    logger.error(
                        "A gravação %s não pôde ser processada "
                        "após %d tentativas.",
                        id_gravacao,
                        max_tentativas
                    )

                    # -------------------------------------------
                    # Salva registro do erro.
                    # -------------------------------------------
                    registro_erro = {
                        "id": id_gravacao,
                        "audio": gravacao["audio"],
                        "metadata": gravacao["metadata"],
                        "erro": str(erro),
                        "tentativas": max_tentativas
                    }

                    # Cria o arquivo de erro na memória
                    arquivo_erro = BytesIO(
                        json.dumps(
                            registro_erro,
                            ensure_ascii=False,
                            indent=2
                        ).encode("utf-8")
                    )

                    try:
                        hook.load_bytes(
                            bytes_data=arquivo_erro.getvalue(),
                            key=(
                                f"erros/silver/"
                                f"{id_gravacao}.json"
                            ),
                            bucket_name=s3_bucket,
                            replace=True
                        )

                        logger.info(
                            "Registro de erro salvo em "
                            "erros/silver/%s.json.",
                            id_gravacao
                        )

                    # Previne erros de gração no S3
                    except Exception as erro_s3:
                        logger.exception(
                            "Não foi possível salvar o registro "
                            "de erro da gravação %s: %s",
                            id_gravacao,
                            erro_s3
                        )

                    return False

            finally:
                # Garante a remoção do arquivo temporário.
                if (caminho_audio is not None and os.path.exists(caminho_audio)):

                    try:
                        os.remove(caminho_audio)

                        logger.info(
                            "Arquivo temporário removido: %s",
                            caminho_audio
                        )

                    except Exception as erro_remocao:
                        logger.warning(
                            "Não foi possível remover o arquivo "
                            "temporário %s: %s",
                            caminho_audio,
                            erro_remocao
                        )

        return False

    @task(
        retries=3,
        retry_delay=pendulum.duration(seconds=30),
        retry_exponential_backoff=True,
        max_retry_delay=pendulum.duration(minutes=5),
        pool="xeno_canto_process"
    )
    def processar_lote(lote):
        """
        Processa um lote de gravações.

        O tamanho do lote é limitado a 10 arquivos para evitar
        problemas com o Dynamic Task Mapping/XCom.

        Cada gravação possui seu próprio mecanismo de retry.
        Portanto, uma gravação com problema não interrompe o
        processamento das demais.
        """

        s3_bucket = Variable.get('S3_Bucket', default=None)

        if not s3_bucket:
            raise AirflowConfigException(
                "A variável 'S3_Bucket' não foi cadastrada no Airflow."
            )

        hook = S3Hook(aws_conn_id='aws_conn')

        sucessos = 0
        falhas = 0

        logger.info("==========================================")
        logger.info("Iniciando lote com %d gravações.",len(lote))

        # --------------------------------------------------------
        # Processa cada gravação do lote individualmente.
        # --------------------------------------------------------
        for gravacao in lote:
            sucesso = processar_uma_gravacao(
                gravacao,
                hook,
                s3_bucket
            )

            if sucesso:
                sucessos += 1
            else:
                falhas += 1

        # --------------------------------------------------------
        # Resultado do lote
        # --------------------------------------------------------
        logger.info("==========================================")
        logger.info("Lote finalizado.")

        logger.info(
            "Sucessos: %d | Falhas: %d",
            sucessos,
            falhas
        )

        # --------------------------------------------------------
        # O lote não falha por causa de um arquivo inválido.
        #
        # Os arquivos problemáticos já foram registrados em
        # erros/silver/.
        #
        # Isso permite que os demais lotes continuem normalmente.
        # --------------------------------------------------------

        if falhas > 0:
            logger.warning(
                "O lote terminou com %d falha(s). "
                "Os arquivos problemáticos foram registrados "
                "em erros/silver/.",
                falhas
            )

        logger.info("==========================================")

    # ============================================================
    # Fluxo da DAG
    # ============================================================

    # Obtém os arquivos da Bronze e os divide em lotes.
    lotes = obter_arquivos_bronze()

    # Dynamic Task Mapping ocorre sobre os lotes,
    # e não sobre cada gravação individual.
    processar_lote.expand(lote=lotes)

# ================================================================
# Instanciação do DAG
# ================================================================

ai_xeno_canto_silver()