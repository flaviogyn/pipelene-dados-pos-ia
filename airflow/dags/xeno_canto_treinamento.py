"""
## DAG Xeno-canto - Treinamento

Esta DAG faz Treinamento de Modelos de IA
"""
from airflow.sdk import dag, task
from pendulum import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowConfigException
from airflow.sdk import Variable
import io
import json
import logging
import os
import tempfile
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

        if not s3_bucket:
            raise AirflowConfigException(
                "A variável 'S3_Bucket' não foi cadastrada no Airflow."
            )

        logger.info("Carregando arquivo do Bucket S3.")
        hook = S3Hook(aws_conn_id='aws_conn')
        dados = hook.download_file(
            key='gold/fct_recordings.parquet',
            bucket_name=s3_bucket,
            local_path="/tmp"
        )

        df = pd.read_parquet(dados)

        # DataFrame não é serializável pelo XCom padrão do Airflow.
        return df.to_json(orient="split")

    @task
    def treinar_modelo_biblioteca(dados):
        import numpy as np
        import tensorflow as tf
        from sklearn.model_selection import train_test_split
        from sklearn.preprocessing import StandardScaler

        df = pd.read_json(io.StringIO(dados), orient="split")

        # Transforma a coluna categórica _COL_0 em colunas numéricas.
        df_COL_0 = pd.get_dummies(
            df['_COL_0'],
            prefix='_COL_0',
            dtype=int
        )
        df = df.drop('_COL_0', axis=1).join(df_COL_0)
        df['_COL_5'] = df['_COL_5'].astype(int)

        # _COL_5 é a variável alvo. Além da _COL_0 codificada, as features
        # acústicas estão a partir da _COL_6.
        colunas_entrada = [
            coluna for coluna in df.columns
            if (
                coluna.startswith('_COL_')
                and coluna.removeprefix('_COL_').isdigit()
                and int(coluna.removeprefix('_COL_')) >= 6
            )
        ]
        colunas_entrada.extend(df_COL_0.columns)

        X = df[colunas_entrada].astype(np.float32)
        y = df['_COL_5']

        X_treino, X_teste, y_treino, y_teste = train_test_split(
            X,
            y,
            test_size=0.2,
            stratify=y,
            random_state=43
        )

        scaler = StandardScaler()
        X_treino = scaler.fit_transform(X_treino)
        X_teste = scaler.transform(X_teste)

        # Mesma arquitetura do código original, usando a biblioteca Keras.
        modelo = tf.keras.Sequential([
            tf.keras.layers.Input(shape=(X_treino.shape[1],)),
            tf.keras.layers.Dense(32, activation='relu'),
            tf.keras.layers.Dense(16, activation='relu'),
            tf.keras.layers.Dense(1, activation='sigmoid')
        ])

        modelo.compile(
            optimizer=tf.keras.optimizers.SGD(
                learning_rate=0.001,
                momentum=0.9
            ),
            loss=tf.keras.losses.BinaryCrossentropy(),
            metrics=['accuracy']
        )

        modelo.fit(
            X_treino,
            y_treino,
            epochs=50,
            verbose=0
        )

        perda, taxa_acerto = modelo.evaluate(
            X_teste,
            y_teste,
            verbose=0
        )

        # O modelo precisa ser salvo nesta task, pois arquivos locais não são
        # compartilhados entre os workers do Celery.
        s3_bucket = Variable.get('S3_Bucket')
        hook = S3Hook(aws_conn_id='aws_conn')

        with tempfile.TemporaryDirectory() as diretorio:
            caminho_modelo = os.path.join(diretorio, 'modelo.keras')
            modelo.save(caminho_modelo)
            hook.load_file(
                filename=caminho_modelo,
                key='treinamentos/modelo.keras',
                bucket_name=s3_bucket,
                replace=True
            )

        return {
            'perda': float(perda),
            'taxa_acerto': float(taxa_acerto),
            'modelo': 'treinamentos/modelo.keras'
        }

    @task
    def treinar_modelo_hardcode(dados):
        import numpy as np
        from sklearn.model_selection import train_test_split
        from sklearn.preprocessing import StandardScaler

        df = pd.read_json(io.StringIO(dados), orient="split")

        # Aplica o mesmo tratamento categórico usado no modelo com biblioteca.
        df_COL_0 = pd.get_dummies(
            df['_COL_0'],
            prefix='_COL_0',
            dtype=int
        )
        df = df.drop('_COL_0', axis=1).join(df_COL_0)
        df['_COL_5'] = df['_COL_5'].astype(int)

        colunas_entrada = [
            coluna for coluna in df.columns
            if (
                coluna.startswith('_COL_')
                and coluna.removeprefix('_COL_').isdigit()
                and int(coluna.removeprefix('_COL_')) >= 6
            )
        ]
        colunas_entrada.extend(df_COL_0.columns)

        X = df[colunas_entrada].astype(np.float32)
        y = df['_COL_5'].to_numpy(dtype=np.float32).reshape(-1, 1)

        X_treino, X_teste, y_treino, y_teste = train_test_split(
            X,
            y,
            test_size=0.2,
            stratify=y.reshape(-1),
            random_state=43
        )

        scaler = StandardScaler()
        X_treino = scaler.fit_transform(X_treino).astype(np.float32)
        X_teste = scaler.transform(X_teste).astype(np.float32)

        # Rede simples: entrada -> camada oculta ReLU -> saída Sigmoid.
        quantidade_features = X_treino.shape[1]
        quantidade_neuronios = 16

        gerador = np.random.default_rng(43)
        pesos_oculta = gerador.normal(
            0,
            np.sqrt(2 / quantidade_features),
            size=(quantidade_features, quantidade_neuronios)
        ).astype(np.float32)
        vies_oculta = np.zeros((1, quantidade_neuronios), dtype=np.float32)
        pesos_saida = gerador.normal(
            0,
            np.sqrt(1 / quantidade_neuronios),
            size=(quantidade_neuronios, 1)
        ).astype(np.float32)
        vies_saida = np.zeros((1, 1), dtype=np.float32)

        def sigmoid(valor):
            # O limite evita overflow no cálculo da exponencial.
            valor = np.clip(valor, -500, 500)
            return 1 / (1 + np.exp(-valor))

        def executar_rede(entrada):
            valor_oculta = entrada @ pesos_oculta + vies_oculta
            camada_oculta = np.maximum(0, valor_oculta)
            probabilidades = sigmoid(
                camada_oculta @ pesos_saida + vies_saida
            )
            return valor_oculta, camada_oculta, probabilidades

        learning_rate = 0.001
        momentum = 0.9
        quantidade_registros = len(X_treino)

        # Velocidades utilizadas pelo SGD com momentum.
        velocidade_pesos_oculta = np.zeros_like(pesos_oculta)
        velocidade_vies_oculta = np.zeros_like(vies_oculta)
        velocidade_pesos_saida = np.zeros_like(pesos_saida)
        velocidade_vies_saida = np.zeros_like(vies_saida)

        for _ in range(50):
            valor_oculta, camada_oculta, probabilidades = executar_rede(
                X_treino
            )

            # Backpropagation da saída para a camada oculta.
            gradiente_saida = (probabilidades - y_treino) / quantidade_registros
            gradiente_pesos_saida = camada_oculta.T @ gradiente_saida
            gradiente_vies_saida = np.sum(
                gradiente_saida,
                axis=0,
                keepdims=True
            )

            gradiente_oculta = (gradiente_saida @ pesos_saida.T) * (
                valor_oculta > 0
            )
            gradiente_pesos_oculta = X_treino.T @ gradiente_oculta
            gradiente_vies_oculta = np.sum(
                gradiente_oculta,
                axis=0,
                keepdims=True
            )

            velocidade_pesos_saida = (
                momentum * velocidade_pesos_saida
                - learning_rate * gradiente_pesos_saida
            )
            velocidade_vies_saida = (
                momentum * velocidade_vies_saida
                - learning_rate * gradiente_vies_saida
            )
            velocidade_pesos_oculta = (
                momentum * velocidade_pesos_oculta
                - learning_rate * gradiente_pesos_oculta
            )
            velocidade_vies_oculta = (
                momentum * velocidade_vies_oculta
                - learning_rate * gradiente_vies_oculta
            )

            pesos_saida += velocidade_pesos_saida
            vies_saida += velocidade_vies_saida
            pesos_oculta += velocidade_pesos_oculta
            vies_oculta += velocidade_vies_oculta

        _, _, probabilidades = executar_rede(X_teste)
        probabilidades_seguras = np.clip(probabilidades, 1e-7, 1 - 1e-7)
        perda_teste = -np.mean(
            y_teste * np.log(probabilidades_seguras)
            + (1 - y_teste) * np.log(1 - probabilidades_seguras)
        )
        predicoes = (probabilidades >= 0.5).astype(np.float32)
        taxa_acerto = np.mean(y_teste == predicoes)

        # Como o modelo é manual, salvamos diretamente pesos e vieses.
        s3_bucket = Variable.get('S3_Bucket')
        hook = S3Hook(aws_conn_id='aws_conn')

        with tempfile.TemporaryDirectory() as diretorio:
            caminho_modelo = os.path.join(diretorio, 'modelo_hardcode.npz')
            np.savez(
                caminho_modelo,
                pesos_oculta=pesos_oculta,
                vies_oculta=vies_oculta,
                pesos_saida=pesos_saida,
                vies_saida=vies_saida,
                media_scaler=scaler.mean_,
                escala_scaler=scaler.scale_
            )
            hook.load_file(
                filename=caminho_modelo,
                key='treinamentos/modelo_hardcode.npz',
                bucket_name=s3_bucket,
                replace=True
            )

        return {
            'perda': float(perda_teste),
            'taxa_acerto': float(taxa_acerto),
            'modelo': 'treinamentos/modelo_hardcode.npz'
        }

    @task
    def salvar_resultados(resultado1, resultado2):
        s3_bucket = Variable.get('S3_Bucket', default=None)

        if not s3_bucket:
            raise AirflowConfigException(
                "A variável 'S3_Bucket' não foi cadastrada no Airflow."
            )

        logger.info("Salvando resultados no Bucket S3.")
        arquivo = io.BytesIO(
            json.dumps({
                'biblioteca': resultado1,
                'hardcode': resultado2
            }).encode('utf-8')
        )

        hook = S3Hook(aws_conn_id='aws_conn')
        hook.load_file_obj(
            file_obj=arquivo,
            key='treinamentos/resultados.json',
            bucket_name=s3_bucket,
            replace=True
        )

    dados = carregar_dados()
    resultado1 = treinar_modelo_biblioteca(dados)
    resultado2 = treinar_modelo_hardcode(dados)
    salvar_resultados(resultado1, resultado2)


# Instanciando o DAG
ai_xeno_canto_treinamento()
