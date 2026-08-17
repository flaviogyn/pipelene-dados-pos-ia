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
        import pandas as pd
        import tensorflow as tf

        from sklearn.model_selection import train_test_split
        from sklearn.preprocessing import StandardScaler

        # Transforma a coluna categórica _COL_0 em colunas numéricas.
        df_COL_0 = pd.get_dummies(
            df['_COL_0'],
            prefix='_COL_0',
            dtype=int
        )

        df = df.drop('_COL_0', axis=1).join(df_COL_0)

        # Variável alvo
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

        # Adiciona as colunas criadas pelo One Hot Encoding
        colunas_entrada.extend(df_COL_0.columns)

        X = df[colunas_entrada].astype(np.float32)

        y = df['_COL_5'].astype(np.float32)

        # SEPARAÇÃO TREINO / VALIDAÇÃO / TESTE

        # 80% para o treinamento e 20% para teste final
        X_treino, X_teste, y_treino, y_teste = train_test_split(
            X,
            y,
            test_size=0.20,
            stratify=y,
            random_state=43
        )

        # Dos 80% restantes, separamos 20% para validação.
        X_treino, X_validacao, y_treino, y_validacao = train_test_split(
            X_treino,
            y_treino,
            test_size=0.20,
            stratify=y_treino,
            random_state=43
        )

        # PADRONIZAÇÃO
        scaler = StandardScaler()

        X_treino = scaler.fit_transform(X_treino)

        X_validacao = scaler.transform(X_validacao)

        X_teste = scaler.transform(X_teste)

        # CRIAÇÃO DA REDE NEURAL
        modelo = tf.keras.Sequential([

            tf.keras.layers.Input(
                shape=(X_treino.shape[1],)
            ),

            tf.keras.layers.Dense(
                32,
                activation='relu'
            ),

            tf.keras.layers.Dense(
                16,
                activation='relu'
            ),

            tf.keras.layers.Dense(
                1,
                activation='sigmoid'
            )

        ])

        # COMPILAÇÃO
        modelo.compile(

            optimizer=tf.keras.optimizers.SGD(
                learning_rate=0.001,
                momentum=0.9 # Ajudando a acelerar e estabilizar o treinamento
            ),

            loss=tf.keras.losses.BinaryCrossentropy(),

            metrics=['accuracy']
        )

        # CHECKPOINT DO MELHOR MODELO
        # Critério de menor validação loss será o melhor modelo

        s3_bucket = Variable.get('S3_Bucket')
        hook = S3Hook(aws_conn_id='aws_conn')

        # O modelo precisa ser salvo nesta task, pois arquivos locais não são
        # compartilhados entre os workers do Celery.
        with tempfile.TemporaryDirectory() as diretorio:

            caminho_checkpoint = os.path.join(
                diretorio,
                'melhor_modelo.keras'
            )

            checkpoint = tf.keras.callbacks.ModelCheckpoint(

                filepath=caminho_checkpoint,

                monitor='val_loss',

                mode='min',

                save_best_only=True,

                verbose=0
            )

        # Interrompe o treinamento quando a loss de validação parar de melhorar
        early_stopping = tf.keras.callbacks.EarlyStopping(
            monitor='val_loss',
            mode='min',

            # Aguarda 20 épocas sem melhora antes de parar
            patience=20,

            # Volta para os pesos da melhor época encontrada
            restore_best_weights=True,

            verbose=1
        )

        # TREINAMENTO
        modelo.fit(

            X_treino,
            y_treino,

            validation_data=(
                X_validacao,
                y_validacao
            ),

            epochs=400,

            callbacks=[
                checkpoint,
                early_stopping
            ],

            verbose=0
        )

        # AVALIAÇÃO FINAL
        # Como restore_best_weights=True, o modelo em memória
        # contém os pesos correspondentes à melhor época.
        perda, taxa_acerto = modelo.evaluate(
            X_teste,
            y_teste,
            verbose=0
        )

        # SALVAR MODELO
        #
        # Apesar do checkpoint já ter salvo o melhor modelo,
        # salvamos novamente o modelo após o EarlyStopping para
        # garantir que o arquivo enviado ao S3 corresponda aos
        # melhores pesos restaurados.

        caminho_modelo = os.path.join(
            diretorio,
            'modelo.keras'
        )

        modelo.save(
            caminho_modelo
        )


        # Envia o modelo para o S3.
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
        import pandas as pd

        from sklearn.model_selection import train_test_split
        from sklearn.preprocessing import StandardScaler

        df = pd.read_json(io.StringIO(dados), orient="split")

        # PREPARAÇÃO DOS DADOS

        # Transforma a coluna categórica _COL_0 em colunas numéricas.
        df_COL_0 = pd.get_dummies(
            df['_COL_0'],
            prefix='_COL_0',
            dtype=int
        )

        df = df.drop('_COL_0', axis=1).join(df_COL_0)

        # Variável alvo
        df['_COL_5'] = df['_COL_5'].astype(int)

        # _COL_5 é a variável alvo. Além da _COL_0 codificada,
        # as features acústicas estão a partir da _COL_6.
        colunas_entrada = [
            coluna for coluna in df.columns
            if (
                coluna.startswith('_COL_')
                and coluna.removeprefix('_COL_').isdigit()
                and int(coluna.removeprefix('_COL_')) >= 6
            )
        ]

        # Adiciona as colunas criadas pelo One Hot Encoding.
        colunas_entrada.extend(df_COL_0.columns)

        X = df[colunas_entrada].astype(np.float32)

        y = (df['_COL_5'].to_numpy(dtype=np.float32).reshape(-1, 1))

        # SEPARAÇÃO TREINO / VALIDAÇÃO / TESTE

        # 80% para treinamento e 20% para teste final
        X_treino, X_teste, y_treino, y_teste = train_test_split(
            X,
            y,
            test_size=0.20,
            stratify=y.reshape(-1),
            random_state=43
        )


        # Dos 80% restantes, separamos 20% para validação.
        X_treino, X_validacao, y_treino, y_validacao = train_test_split(
            X_treino,
            y_treino,
            test_size=0.20,
            stratify=y_treino.reshape(-1),
            random_state=43
        )

        # PADRONIZAÇÃO
        scaler = StandardScaler()

        X_treino = scaler.fit_transform(X_treino).astype(np.float32)

        X_validacao = scaler.transform(X_validacao).astype(np.float32)

        X_teste = scaler.transform(X_teste).astype(np.float32)

        # CRIAÇÃO DA REDE NEURAL
        # Entrada
        #   ↓
        # 32 neurônios + ReLU
        #   ↓
        # 16 neurônios + ReLU
        #   ↓
        # 1 neurônio + Sigmoid

        quantidade_features = X_treino.shape[1]

        quantidade_neuronios_1 = 32
        quantidade_neuronios_2 = 16

        # Gerador utilizado para inicialização dos pesos.
        gerador = np.random.default_rng(43)

        # Primeira camada oculta
        pesos_oculta_1 = gerador.normal(
            0,
            np.sqrt(
                2 / quantidade_features
            ),
            size=(
                quantidade_features,
                quantidade_neuronios_1
            )
        ).astype(np.float32)


        vies_oculta_1 = np.zeros(
            (
                1,
                quantidade_neuronios_1
            ),
            dtype=np.float32
        )

        # Segunda camada oculta
        pesos_oculta_2 = gerador.normal(
            0,
            np.sqrt(
                2 / quantidade_neuronios_1
            ),
            size=(
                quantidade_neuronios_1,
                quantidade_neuronios_2
            )
        ).astype(np.float32)


        vies_oculta_2 = np.zeros(
            (
                1,
                quantidade_neuronios_2
            ),
            dtype=np.float32
        )

        # Camada de saída
        pesos_saida = gerador.normal(
            0,
            np.sqrt(
                1 / quantidade_neuronios_2
            ),
            size=(
                quantidade_neuronios_2,
                1
            )
        ).astype(np.float32)


        vies_saida = np.zeros(
            (1, 1),
            dtype=np.float32
        )

        # FUNÇÕES DA REDE
        def sigmoid(valor):

            # O limite evita overflow no cálculo da exponencial.
            valor = np.clip(
                valor,
                -500,
                500
            )

            return 1 / (
                1 + np.exp(-valor)
            )


        def executar_rede(entrada):

            # Primeira camada oculta.
            valor_oculta_1 = (
                entrada
                @ pesos_oculta_1
                + vies_oculta_1
            )

            camada_oculta_1 = np.maximum(
                0,
                valor_oculta_1
            )

            # Segunda camada oculta.
            valor_oculta_2 = (
                camada_oculta_1
                @ pesos_oculta_2
                + vies_oculta_2
            )

            camada_oculta_2 = np.maximum(
                0,
                valor_oculta_2
            )

            # Camada de saída.
            probabilidades = sigmoid(
                camada_oculta_2
                @ pesos_saida
                + vies_saida
            )

            return (
                valor_oculta_1,
                camada_oculta_1,
                valor_oculta_2,
                camada_oculta_2,
                probabilidades
            )

        def calcular_loss(y_real, probabilidades):

            # Evita log(0) no cálculo da Binary Cross-Entropy.
            probabilidades_seguras = np.clip(
                probabilidades,
                1e-7,
                1 - 1e-7
            )


            perda = -np.mean(
                y_real
                * np.log(
                    probabilidades_seguras
                )
                +
                (1 - y_real)
                * np.log(
                    1 - probabilidades_seguras
                )
            )


            return float(
                perda
            )

        # CONFIGURAÇÃO DO TREINAMENTO
        learning_rate = 0.001
        momentum = 0.9
        batch_size = 32
        max_epocas = 400

        # Primeira camada oculta
        velocidade_pesos_oculta_1 = np.zeros_like(
            pesos_oculta_1
        )

        velocidade_vies_oculta_1 = np.zeros_like(
            vies_oculta_1
        )

        # Segunda camada oculta
        velocidade_pesos_oculta_2 = np.zeros_like(
            pesos_oculta_2
        )

        velocidade_vies_oculta_2 = np.zeros_like(
            vies_oculta_2
        )

        # Camada de saída
        velocidade_pesos_saida = np.zeros_like(
            pesos_saida
        )

        velocidade_vies_saida = np.zeros_like(
            vies_saida
        )

        # EARLY STOPPING
        # Interrompe o treinamento quando a loss de validação parar de melhorar por 20 épocas.
        patience = 20
        melhor_val_loss = np.inf
        epocas_sem_melhora = 0

        # Armazena os pesos correspondentes à menor loss de validação encontrada.
        melhores_pesos = None

        # TREINAMENTO
        quantidade_registros = len(X_treino)

        # Gerador utilizado para embaralhar os registros antes de cada época.
        gerador_batch = np.random.default_rng(43)

        for epoca in range(
            1,
            max_epocas + 1
        ):

            # Embaralhamento dos registros
            indices = gerador_batch.permutation(
                quantidade_registros
            )

            # Treinamento utilizando mini-batches
            for inicio in range(
                0,
                quantidade_registros,
                batch_size
            ):

                fim = (
                    inicio
                    + batch_size
                )


                indices_batch = indices[
                    inicio:fim
                ]


                X_batch = X_treino[
                    indices_batch
                ]

                y_batch = y_treino[
                    indices_batch
                ]


                quantidade_batch = len(
                    X_batch
                )


                # FORWARD PROPAGATION
                (
                    valor_oculta_1,
                    camada_oculta_1,
                    valor_oculta_2,
                    camada_oculta_2,
                    probabilidades
                ) = executar_rede(
                    X_batch
                )

                # BACKPROPAGATION DA CAMADA DE SAÍDA
                gradiente_saida = (
                    probabilidades
                    - y_batch
                ) / quantidade_batch


                gradiente_pesos_saida = (
                    camada_oculta_2.T
                    @ gradiente_saida
                )


                gradiente_vies_saida = np.sum(
                    gradiente_saida,
                    axis=0,
                    keepdims=True
                )

                # BACKPROPAGATION DA SEGUNDA CAMADA OCULTA
                gradiente_oculta_2 = (
                    gradiente_saida
                    @ pesos_saida.T
                ) * (
                    valor_oculta_2 > 0
                )


                gradiente_pesos_oculta_2 = (
                    camada_oculta_1.T
                    @ gradiente_oculta_2
                )


                gradiente_vies_oculta_2 = np.sum(
                    gradiente_oculta_2,
                    axis=0,
                    keepdims=True
                )

                # BACKPROPAGATION DA PRIMEIRA CAMADA OCULTA
                gradiente_oculta_1 = (
                    gradiente_oculta_2
                    @ pesos_oculta_2.T
                ) * (
                    valor_oculta_1 > 0
                )


                gradiente_pesos_oculta_1 = (
                    X_batch.T
                    @ gradiente_oculta_1
                )


                gradiente_vies_oculta_1 = np.sum(
                    gradiente_oculta_1,
                    axis=0,
                    keepdims=True
                )

                # SGD COM MOMENTUM
                velocidade_pesos_saida = (
                    momentum
                    * velocidade_pesos_saida
                    -
                    learning_rate
                    * gradiente_pesos_saida
                )


                velocidade_vies_saida = (
                    momentum
                    * velocidade_vies_saida
                    -
                    learning_rate
                    * gradiente_vies_saida
                )


                velocidade_pesos_oculta_2 = (
                    momentum
                    * velocidade_pesos_oculta_2
                    -
                    learning_rate
                    * gradiente_pesos_oculta_2
                )


                velocidade_vies_oculta_2 = (
                    momentum
                    * velocidade_vies_oculta_2
                    -
                    learning_rate
                    * gradiente_vies_oculta_2
                )


                velocidade_pesos_oculta_1 = (
                    momentum
                    * velocidade_pesos_oculta_1
                    -
                    learning_rate
                    * gradiente_pesos_oculta_1
                )


                velocidade_vies_oculta_1 = (
                    momentum
                    * velocidade_vies_oculta_1
                    -
                    learning_rate
                    * gradiente_vies_oculta_1
                )

                # ATUALIZAÇÃO DOS PESOS E VIESES
                pesos_saida += (
                    velocidade_pesos_saida
                )

                vies_saida += (
                    velocidade_vies_saida
                )


                pesos_oculta_2 += (
                    velocidade_pesos_oculta_2
                )

                vies_oculta_2 += (
                    velocidade_vies_oculta_2
                )


                pesos_oculta_1 += (
                    velocidade_pesos_oculta_1
                )

                vies_oculta_1 += (
                    velocidade_vies_oculta_1
                )

            # AVALIAÇÃO DA ÉPOCA NO CONJUNTO DE VALIDAÇÃO
            (
                _,
                _,
                _,
                _,
                probabilidades_validacao
            ) = executar_rede(
                X_validacao
            )


            loss_validacao = calcular_loss(
                y_validacao,
                probabilidades_validacao
            )

            # VERIFICAÇÃO DO EARLY STOPPING
            if loss_validacao < melhor_val_loss:

                melhor_val_loss = (
                    loss_validacao
                )

                epocas_sem_melhora = 0


                # Salva uma cópia dos pesos da melhor época.
                melhores_pesos = {

                    'pesos_oculta_1':
                        pesos_oculta_1.copy(),

                    'vies_oculta_1':
                        vies_oculta_1.copy(),

                    'pesos_oculta_2':
                        pesos_oculta_2.copy(),

                    'vies_oculta_2':
                        vies_oculta_2.copy(),

                    'pesos_saida':
                        pesos_saida.copy(),

                    'vies_saida':
                        vies_saida.copy()
                }


            else:
                epocas_sem_melhora += 1

            # Interrompe o treinamento após atingir a quantidade definida em patience.
            if epocas_sem_melhora >= patience:
                break

        # RESTAURAR O MELHOR MODELO
        pesos_oculta_1 = melhores_pesos[
            'pesos_oculta_1'
        ]

        vies_oculta_1 = melhores_pesos[
            'vies_oculta_1'
        ]


        pesos_oculta_2 = melhores_pesos[
            'pesos_oculta_2'
        ]

        vies_oculta_2 = melhores_pesos[
            'vies_oculta_2'
        ]


        pesos_saida = melhores_pesos[
            'pesos_saida'
        ]

        vies_saida = melhores_pesos[
            'vies_saida'
        ]
        
        # AVALIAÇÃO FINAL
        (
            _,
            _,
            _,
            _,
            probabilidades_teste
        ) = executar_rede(
            X_teste
        )

        perda_teste = calcular_loss(
            y_teste,
            probabilidades_teste
        )

        taxa_acerto = np.mean(
            (probabilidades_teste >= 0.5)
            == y_teste
        )

        # SALVAR MODELO

        # Como o modelo é manual, salvamos diretamente
        # pesos, vieses e dados necessários para a inferência.
        s3_bucket = Variable.get('S3_Bucket')
        hook = S3Hook(aws_conn_id='aws_conn')

        with tempfile.TemporaryDirectory() as diretorio:

            caminho_modelo = os.path.join(
                diretorio,
                'modelo_hardcode.npz'
            )

            np.savez(
                caminho_modelo,

                # Primeira camada oculta
                pesos_oculta_1=pesos_oculta_1,
                vies_oculta_1=vies_oculta_1,

                # Segunda camada oculta
                pesos_oculta_2=pesos_oculta_2,
                vies_oculta_2=vies_oculta_2,

                # Camada de saída
                pesos_saida=pesos_saida,
                vies_saida=vies_saida,

                # StandardScaler
                media_scaler=scaler.mean_,
                escala_scaler=scaler.scale_,

                # Ordem exata das features utilizadas no treinamento
                colunas_entrada=np.array(
                    colunas_entrada,
                    dtype=str
                )
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
