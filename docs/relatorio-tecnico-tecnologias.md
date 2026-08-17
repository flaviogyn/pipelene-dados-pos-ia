# Relatorio tecnico do uso das tecnologias

Projeto: `pipelene-dados-pos-ia`

Ultima atualizacao deste relatorio: 2026-08-16.

Escopo analisado: `airflow/`, `dbt/`, `docker/`, `metabase/`, `snowflake/` e `s3/`.

## 1. Visao geral da arquitetura

O projeto implementa um pipeline de dados e IA para gravacoes da base Xeno-Canto. A arquitetura usa um desenho em camadas:

- `S3`: data lake com zonas bronze, silver e gold.
- `Airflow`: orquestracao das etapas de ingestao, processamento, transformacao e treinamento.
- `dbt`: transformacoes SQL, carga controlada da silver no Snowflake e publicacao da gold.
- `Snowflake`: data warehouse central, com stages externos para ler e escrever no S3.
- `Metabase`: camada de BI e visualizacao sobre os dados finais.
- `Docker`: empacotamento dos ambientes de Airflow, dbt e Metabase.

Tambem ha uma pasta `docs/` com materiais visuais e referencias de arquitetura, incluindo `Data Pipeline & Analytics Architecture.jpg`, alem deste relatorio.

Fluxo tecnico principal:

1. Airflow consulta a API Xeno-Canto.
2. Airflow grava JSONs de consulta, audios e metadados na camada bronze do S3.
3. Airflow processa os audios com `librosa` e gera Parquets por gravacao na camada silver.
4. Airflow aciona dbt remotamente via SSH.
5. dbt executa macro de `MERGE` da silver para uma tabela RAW no Snowflake.
6. dbt cria view de staging e tabelas marts/gold.
7. dbt exporta tabelas gold para S3.
8. Metabase consome as tabelas do Snowflake e possui painel Xeno-Canto documentado.
9. Airflow usa `gold/fct_recordings.parquet` para treinamento de modelos.

## 2. Airflow

### Papel no projeto

O Airflow e o orquestrador principal. Ele coordena chamadas externas, leitura e escrita em S3, processamento de audio, execucao remota do dbt e treinamento de modelos.

A configuracao em `airflow/docker-compose.yml` usa Airflow 3.1.1 com `CeleryExecutor`, PostgreSQL como metastore e Redis como broker. A imagem e customizada em `airflow/Dockerfile`, a partir de `apache/airflow:3.1.1`, instalando as dependencias de `airflow/requirements.txt`.

### Componentes Docker do Airflow

O compose define:

- `postgres`: banco de metadados do Airflow.
- `redis`: broker do Celery.
- `airflow-apiserver`: API/interface na porta 8080.
- `airflow-scheduler`: agendamento.
- `airflow-worker`: execucao das tasks.
- `airflow-triggerer`: suporte a triggers.
- `airflow-dag-processor`: processamento de DAGs.
- `airflow-init`: migracao/inicializacao.
- `flower`: monitoramento opcional dos workers.

O worker monta uma chave SSH em `/home/airflow/.ssh/airflow_to_dbt`, usada pelas DAGs que chamam dbt em outra VPS.

### Dependencias

`airflow/requirements.txt` inclui:

- `pandas`
- `librosa`
- `soundfile`
- `pyarrow`
- `numpy`
- `tensorflow`
- `scikit-learn`

Essas bibliotecas permitem transformar audio em features, criar arquivos Parquet e treinar modelos.

### DAG bronze

Arquivo: `airflow/dags/xeno_canto_bronze_dag.py`

Responsabilidade:

- Le a configuracao `xeno_canto_config` do Airflow Variables.
- Para cada especie configurada, monta uma query Xeno-Canto.
- Chama a API `https://xeno-canto.org/api/3/recordings`.
- Usa `per_page=500`, ampliando a quantidade de resultados retornados por consulta.
- Grava o JSON completo da consulta em `bronze/query/<query>.json`.
- Seleciona a quantidade desejada de gravacoes por especie.
- Baixa cada audio.
- Grava audio em `bronze/audio/<id>.<extensao>`.
- Grava metadado individual em `bronze/metadata/<id>.json`.

Pontos tecnicos:

- Usa `Dynamic Task Mapping` para paralelizar consultas e downloads.
- Usa `S3Hook` com a connection `aws_conn`.
- Usa pool `xeno_canto_download` para controlar concorrencia.
- Possui retry com backoff exponencial no download.
- Agora importa `AirflowException`, corrigindo o risco anterior de excecao nao importada.

Ponto de atencao ainda existente:

- As mensagens de erro ainda citam `s3_bucket` em alguns lugares, mas a variavel real usada e `S3_Bucket`.

### DAG silver

Arquivo: `airflow/dags/xeno_canto_silver_dag.py`

Responsabilidade:

- Lista arquivos em `bronze/audio/` e `bronze/metadata/`.
- Relaciona audio e metadado pelo ID.
- Divide as gravacoes em lotes de 10.
- Processa cada lote com Dynamic Task Mapping.
- Para cada gravacao, baixa o audio, extrai features, padroniza campos e salva Parquet em `silver/<id>.parquet`.

Mudanca relevante observada:

- A DAG deixou de mapear uma task por gravacao e passou a mapear uma task por lote.
- Cada gravacao dentro do lote tem retry interno de ate 3 tentativas.
- Falhas por gravacao nao derrubam necessariamente o lote inteiro.
- Erros persistentes sao registrados em `erros/silver/<id>.json`.
- Arquivos temporarios baixados para `/tmp` sao removidos no `finally`.
- Datas invalidas sao convertidas para a data atual normalizada e exportadas em `YYYY-MM-DD`.

Features extraidas:

- `duration_seconds`
- `energy_mean`
- `rms_mean`
- `rms_std`
- `zcr_mean`
- `zcr_std`
- `spectral_centroid_mean`
- `spectral_bandwidth_mean`
- `mfcc_1_mean` ate `mfcc_20_mean`
- `mfcc_1_std` ate `mfcc_20_std`

Campos de metadados incluidos na silver:

- `id`
- `gen`
- `sp`
- `ssp`
- `en`
- `status`
- `cnt`
- `loc`
- `q`
- `url`
- `date`
- `type`
- `lat`
- `lon`
- `alt`
- `grp`

Pontos fortes:

- Melhor controle de XCom e Dynamic Task Mapping ao trabalhar com lotes.
- Tratamento mais robusto de erros por arquivo.
- Registro dos arquivos problematicos em uma area especifica no S3.
- Limpeza de arquivos temporarios.

Pontos de atencao:

- O tamanho do lote esta fixo em 10; pode virar variavel Airflow para ajuste operacional.
- O fallback de datas invalidas para a data atual evita falha, mas pode mascarar problema de qualidade de dado.

### DAG gold/dbt

Arquivo: `airflow/dags/xeno_canto_gold_dag.py`

Responsabilidade:

- Acionar dbt remotamente via `SSHOperator`.
- Executar carga silver no Snowflake.
- Rodar modelos staging e marts.
- Exportar tabelas gold de volta ao S3.

Fluxo atual:

1. `dbt run-operation load_silver_features`
2. `dbt run --select staging`
3. `dbt run --select marts`
4. `dbt run-operation unload_fct_recordings`
5. `dbt run-operation unload_dim_species`

Ponto de atencao:

- A task `dbt_test` existe no codigo, mas esta comentada. Assim, a DAG principal de gold ainda pode publicar dados sem executar validacoes dbt.

### DAG run_dbt_snowflake

Arquivo: `airflow/dags/run_dbt_snowflake.py`

DAG auxiliar para executar `dbt run` e `dbt test` via SSH no ambiente remoto do dbt. Ela continua util para validacao manual ou execucoes pontuais.

### DAG treinamento

Arquivo: `airflow/dags/xeno_canto_treinamento.py`

Responsabilidade:

- Orquestra o pipeline completo de treinamento de modelos de IA.
- Baixa `gold/fct_recordings.parquet` do S3 como fonte de dados.
- Treina dois modelos de rede neural em paralelo:
  - **Modelo Keras/TensorFlow**: implementacao com bibliotecas, usando SGD com momentum.
  - **Modelo NumPy (hardcode)**: implementacao manual da rede neural sem dependencias de ML.
- Salva artefatos de modelo e resultados no S3.
- Utiliza Airflow Tasks para paralelizacao e gerencia de dependencias.

Fluxo de tasks:

1. **carregar_dados()**: Baixa o arquivo Parquet `gold/fct_recordings.parquet` do S3 usando S3Hook. Converte o DataFrame para JSON (orient="split") para permitir serializacao via XCom entre tasks distribuidas.

2. **treinar_modelo_biblioteca(dados)**: Treina modelo TensorFlow/Keras.
   - Transforma a coluna categorica `_COL_0` via `pd.get_dummies`.
   - Define `_COL_5` como variavel alvo (convertida para inteiro).
   - Seleciona features numericas a partir de `_COL_6` em diante.
   - Divide dados em treino (64%), validacao (16%) e teste (20%).
   - Aplica StandardScaler para normalizacao.
   - Modelo: Input -> Dense(32, ReLU) -> Dense(16, ReLU) -> Dense(1, Sigmoid).
   - Optimizer: SGD com learning_rate=0.001 e momentum=0.9.
   - Loss: BinaryCrossentropy.
   - Callbacks: ModelCheckpoint (salva melhor modelo) e EarlyStopping (patience=20).
   - Salva melhor modelo em `treinamentos/modelo.keras`.
   - Retorna: perda (loss) e taxa de acerto no conjunto de teste.

3. **treinar_modelo_hardcode(dados)**: Treina modelo rede neural com NumPy puro.
   - Mesmo preprocessamento de dados que o modelo Keras.
   - Arquitetura identica (input -> 32 neuronios -> 16 neuronios -> 1 neuronio de saida).
   - Implementacao manual de forward propagation, backpropagation e SGD.
   - Early stopping manual com patience=20.
   - Salva pesos, vieses, parametros do StandardScaler e lista de features em `treinamentos/modelo_hardcode.npz`.
   - Retorna: perda e taxa de acerto no conjunto de teste.

4. **salvar_resultados(resultado1, resultado2)**: Agrupa resultados de ambos os modelos em JSON.
   - Estrutura: `{"biblioteca": {...}, "hardcode": {...}}`.
   - Cada entrada contem: `perda` (float), `taxa_acerto` (float), `modelo` (caminho S3).
   - Salva em `treinamentos/resultados.json`.

Mudanca relevante observada:

- A coluna categorica `_COL_0` (antes `_COL_6`) agora e transformada via `pd.get_dummies`.
- `_COL_5` (antes `_COL_16`) e convertida para inteiro e usada como alvo.
- As features numericas continuam sendo selecionadas a partir de `_COL_6` (antes `_COL_17`).
- As colunas dummy geradas de `_COL_0` sao adicionadas ao conjunto de entrada.
- Ambos os modelos agora usam a mesma estrategia de train/validation/test split e normalizacao para comparacao justa.

Artefatos gerados em `s3/treinamentos/`:

- `modelo.keras`: Arquivo de modelo TensorFlow/Keras com melhor performance durante treinamento.
- `modelo_hardcode.npz`: Arquivo NPZ contendo pesos, vieses e parametros de normalizacao do modelo NumPy.
- `resultados.json`: JSON com metricas finais de ambos os modelos (perda e taxa de acerto no conjunto de teste).

Ponto de atencao:

- O modelo ainda depende de nomes `_COL_*`, que aparecem no Parquet gold exportado pelo Snowflake. Isso funciona com o arquivo atual, mas e menos explicito do que usar nomes semanticos de colunas.
- Ambos os modelos usam `random_state=43` para reprodutibilidade.
- O modelo Keras depende de TensorFlow, enquanto o modelo hardcode depende apenas de NumPy.
- Os resultados podem divergir entre os dois modelos devido a diferencas em implementacao de SGD, inicializacao de pesos e regularizacao.

## 3. dbt

### Papel no projeto

O dbt executa as transformacoes analiticas no Snowflake e tambem encapsula operacoes de carga e exportacao por macros.

Configuracao principal:

- Projeto: `dbt/dbt_project.yml`
- Profile: `pipelene_dbt`
- Target principal: `dev_snowflake`
- Pacote externo: `dbt_utils`

### Profiles e ambiente

`dbt/profiles.yml` define o target `dev_snowflake` usando variaveis de ambiente:

- `DBT_SNOWFLAKE_ACCOUNT`
- `DBT_SNOWFLAKE_USER`
- `DBT_SNOWFLAKE_PASSWORD`
- `DBT_SNOWFLAKE_ROLE`
- `DBT_SNOWFLAKE_DATABASE`
- `DBT_SNOWFLAKE_WAREHOUSE`
- `DBT_SNOWFLAKE_SCHEMA`

O `requirements.txt` inclui adaptadores `dbt-postgres`, `dbt-snowflake` e `dbt-oracle`, mas o fluxo do projeto analisado usa Snowflake.

Ponto de atencao:

- `dbt/docker-compose.yml` usa default `DBT_TARGET=dev_snowflake`, enquanto `dbt/.env.example` ainda sugere `DBT_TARGET=dev`. Se alguem seguir o exemplo literalmente, o target pode nao existir no `profiles.yml`.

### Modelos staging

`dbt/models/staging/sources.yml`

Define a source `xeno_raw`:

- database: `XENO_DB`
- schema: `RAW`
- tabela: `src_recording_features`

Tambem declara testes em `id`, como `not_null` e `unique`.

`dbt/models/staging/stg_recording_features.sql`

Cria uma view de staging sobre `RAW.SRC_RECORDING_FEATURES`, renomeando campos tecnicos para nomes analiticos:

- `id` vira `recording_id`
- `gen` vira `genus`
- `sp` vira `species`
- `cnt` vira `country`
- `q` vira `quality_rating`
- features acusticas sao propagadas para a gold

### Modelos marts/gold

`dbt/models/marts/dim_species.sql`

Cria dimensao de especies com:

- `species_key` via `dbt_utils.generate_surrogate_key`
- `genus`
- `species`
- `subspecies`
- `common_name`
- `is_target_species`

`dbt/models/marts/fct_recordings.sql`

Cria fato com uma linha por gravacao. A tabela contem metadados, localizacao, label binaria `is_target_species` e features acusticas.

`dbt/models/marts/marts.yml`

Define testes:

- `not_null`
- `unique`
- `accepted_values` para `quality_rating`

### Macros

`dbt/macros/load_silver_features.sql`

Executa `MERGE` da silver no Snowflake, lendo Parquets no stage `XENO_DB.RAW.S3_STAGE_SILVER`.

Mudanca relevante observada:

- O campo `date` agora e tratado com:

```sql
COALESCE(
    TRY_TO_DATE($1:date::STRING, 'YYYY-MM-DD'),
    CURRENT_DATE()
)
```

Isso evita erro em datas invalidas, mas tambem substitui valores problematicos pela data corrente.

`dbt/macros/unload_fct_recordings.sql`

Exporta `fct_recordings` para `S3_STAGE_GOLD/fct_recordings.parquet`.

Mudanca relevante observada:

- A macro agora usa `SELECT * EXCLUDE (...)` para remover colunas identificadoras/textuais antes do unload:
  - `recording_id`
  - `genus`
  - `species`
  - `subspecies`
  - `common_name`
  - `identification_status`
  - `location`
  - `quality_rating`
  - `xeno_canto_url`
  - `recording_date`
  - `recording_type`

Objetivo tecnico provavel:

- Gerar um dataset gold mais apropriado para treinamento, reduzindo colunas textuais ou de alta cardinalidade.

Ponto de atencao:

- O Parquet gold local atual ainda apresenta colunas nomeadas como `_COL_0` ate `_COL_63`, indicando que a exportacao via `COPY INTO FROM (SELECT ...)` pode gerar nomes automaticos ou que o arquivo local foi produzido antes/fora da macro atual. A DAG de treinamento esta adaptada a esse padrao `_COL_*`.

`dbt/macros/unload_dim_species.sql`

Exporta `dim_species` para `S3_STAGE_GOLD/dim_species.parquet` com `SINGLE = TRUE` e `OVERWRITE = TRUE`.

### Avaliacao tecnica do dbt

Pontos fortes:

- Separacao clara entre staging e marts.
- Uso de `MERGE` para carga idempotente da silver.
- Uso de `dbt_utils` para chave surrogate.
- Testes dbt documentados para chaves e qualidade basica.
- Macro de unload da fact agora considera necessidade de dataset mais limpo para ML.

Pontos de atencao:

- `dbt_project.yml` materializa marts no schema `gold`, mas `snowflake/setup_snowflake_xeno.sql` cria e concede permissao no schema `CORE`. Essa divergencia ainda precisa ser resolvida.
- `sources.yml` fixa `database: XENO_DB`, o que reduz flexibilidade entre ambientes.
- `SELECT * EXCLUDE` pode depender de comportamento especifico do Snowflake e ainda precisa ser validado contra o schema final esperado no Parquet.
- `dbt_test` nao esta ativo no fluxo gold principal.

## 4. Docker

### Papel no projeto

Docker padroniza a execucao dos tres principais servicos de runtime: Airflow, dbt e Metabase.

### Airflow

`airflow/docker-compose.yml` define um ambiente completo com CeleryExecutor. A imagem customizada instala dependencias de audio, dados e ML. Isso torna o ambiente autocontido, mas aumenta o peso da imagem, especialmente por causa de TensorFlow.

### dbt

`dbt/Dockerfile` parte de `python:3.10-slim`, instala dependencias do dbt e define:

- `WORKDIR /usr/app`
- `DBT_PROFILES_DIR=/usr/app`
- `ORA_PYTHON_DRIVER_TYPE=thin`

`dbt/docker-compose.yml` monta:

- `.` em `/usr/app`
- `../s3` em `/workspace/s3`

O entrypoint e `dbt`, permitindo executar comandos como:

- `docker compose run --rm dbt debug`
- `docker compose run --rm dbt run`
- `docker compose run --rm dbt test`
- `docker compose run --rm dbt run-operation load_silver_features`

### Metabase

`metabase/docker-compose.yml` usa:

- imagem `metabase/metabase:latest`
- porta `${MB_PORT:-3000}`
- banco interno H2
- volume `metabase_data`
- healthcheck em `/api/health`

### Instalacao Docker em VPS

`docker/install-docker-ubuntu.sh` automatiza instalacao do Docker em Ubuntu:

- valida que o sistema e Ubuntu;
- instala pacotes base;
- configura repositorio oficial Docker;
- instala Docker CE, CLI, containerd, Buildx e Compose Plugin;
- habilita/inicia o servico;
- adiciona usuario ao grupo `docker`, quando aplicavel;
- habilita UFW e libera porta 22.

### Avaliacao tecnica do Docker

Pontos fortes:

- Separacao de ambientes por responsabilidade.
- Airflow completo para orquestracao distribuida.
- dbt simples como runner de comandos.
- Metabase com persistencia via volume.

Pontos de atencao:

- `metabase/metabase:latest` nao fixa versao.
- Airflow com TensorFlow pode ter build lento e consumo alto de memoria.
- O uso de SSH entre VPS exige controle operacional de chaves e firewall.

## 5. Snowflake

### Papel no projeto

Snowflake e o data warehouse do projeto. Ele recebe dados silver via external stage, armazena a tabela RAW tipada e executa os modelos dbt para disponibilizar staging/gold.

### Objetos definidos em setup

`snowflake/setup_snowflake_xeno.sql` cria:

- Warehouse `XENO_WH`
- Database `XENO_DB`
- Schemas `RAW`, `STAGING` e `CORE`
- Stage `XENO_DB.RAW.S3_STAGE_SILVER`
- Stage `XENO_DB.RAW.S3_STAGE_GOLD`
- Tabela `XENO_DB.RAW.SRC_RECORDING_FEATURES`
- Role `METABASE_RO`
- Usuario `METABASE_SVC`

O README do Snowflake agora tambem documenta a execucao do setup inicial pelo script `setup_snowflake_xeno.sql`.

### Integracao com S3

Stages:

- `S3_STAGE_SILVER`: aponta para `s3://xeno-canto-s3/silver/`
- `S3_STAGE_GOLD`: aponta para `s3://xeno-canto-s3/gold/`

Uso:

- Leitura da silver para `RAW.SRC_RECORDING_FEATURES`.
- Escrita de datasets gold por `COPY INTO`.

### Tabela RAW

`SRC_RECORDING_FEATURES` possui colunas tipadas para metadados e features acusticas. Isso reduz dependencia de dados semiestruturados e facilita os modelos dbt.

### Seguranca e acesso

O script cria role read-only para Metabase. A intencao e restringir BI apenas aos dados finais. Contudo, a permissao esta apontando para `XENO_DB.CORE`, enquanto o dbt esta configurado para schema `gold`.

### Avaliacao tecnica do Snowflake

Pontos fortes:

- Warehouse pequeno e com auto suspend.
- Stages separados para entrada silver e saida gold.
- Tabela RAW fortemente tipada.
- Usuario e role dedicados para Metabase.

Pontos de atencao:

- O script ainda usa credenciais AWS diretamente no stage; Storage Integration seria mais adequado.
- Divergencia `CORE` vs `gold` permanece.
- O comentario do warehouse menciona 60 segundos, mas `AUTO_SUSPEND = 120`.

## 6. S3

### Papel no projeto

O S3 e o data lake do pipeline. A pasta `s3/` simula/espelha a estrutura do bucket com dados reais de amostra.

### Estado atual da amostra local

Bronze:

- `s3/broze/audio/`: 15 arquivos de audio.
- Distribuicao de audio:
  - 7 arquivos `.wav`
  - 8 arquivos `.mp3`
- `s3/broze/metatada/`: 15 JSONs individuais de metadados.
- `s3/broze/query/`: 3 JSONs de consulta:
  - `sp-pitangus-sulphuratus.json`
  - `sp-sporophila-angolensis.json`
  - `sp-turdus-rufiventris.json`

Silver:

- `s3/silver/`: 15 arquivos Parquet, um por gravacao.
- Cada Parquet silver analisado possui 1 linha e 64 colunas.
- O schema contem metadados, data em string normalizada, coordenadas e features acusticas.

Gold:

- `s3/gold/dim_species.parquet`: 22 linhas e 6 colunas.
- `s3/gold/fct_recordings.parquet`: 1192 linhas e 64 colunas. Esta tabela e a fonte principal para o treinamento de modelos de IA.

Treinamentos:

- `s3/treinamentos/modelo.keras`: Modelo treinado com TensorFlow/Keras. Arquivo de extensao `.keras` contendo pesos, arquitetura e hyperparametros do modelo.
- `s3/treinamentos/modelo_hardcode.npz`: Modelo treinado com NumPy puro. Arquivo NPZ (formato comprimido de NumPy) contendo:
  - Pesos de todas as camadas (oculta 1, oculta 2 e saida).
  - Vieses de todas as camadas.
  - Parametros do StandardScaler (media e escala).
  - Lista de nomes das features na ordem exata utilizada no treinamento.
- `s3/treinamentos/resultados.json`: Arquivo JSON com metricas de desempenho de ambos os modelos. Estrutura esperada:
  ```json
  {
    "biblioteca": {
      "perda": <float>,
      "taxa_acerto": <float>,
      "modelo": "treinamentos/modelo.keras"
    },
    "hardcode": {
      "perda": <float>,
      "taxa_acerto": <float>,
      "modelo": "treinamentos/modelo_hardcode.npz"
    }
  }
  ```

### Observacao sobre nomenclatura local

A pasta local ainda usa `broze` e `metatada`, enquanto as DAGs usam `bronze` e `metadata`. Portanto:

- No codigo Airflow, os paths esperados sao `bronze/audio`, `bronze/metadata` e `bronze/query`.
- Na amostra local, os paths aparecem como `s3/broze/audio`, `s3/broze/metatada` e `s3/broze/query`.

Isso deve ser tratado como divergencia de simulacao local/documentacao versus bucket real.

### Avaliacao tecnica do S3

Pontos fortes:

- Amostra local cresceu e agora representa melhor o pipeline.
- Silver esta consistente como um Parquet por gravacao.
- Gold possui dataset consolidado para BI/ML.

Pontos de atencao:

- Corrigir `broze` para `bronze` e `metatada` para `metadata` se essa estrutura local for usada como referencia operacional.
- O repositorio guarda arquivos de audio e Parquet; para dados maiores, Git LFS ou armazenamento externo seria mais adequado.
- O Parquet gold exportado pelo Snowflake possui colunas `_COL_*`, o que reduz legibilidade e acopla a DAG de treinamento a posicoes de coluna.

## 7. Metabase

### Papel no projeto

Metabase e a ferramenta de BI e visualizacao. Ele deve se conectar ao Snowflake usando credenciais controladas e role read-only.

### Configuracao Docker

`metabase/docker-compose.yml` sobe um container unico com:

- `metabase/metabase:latest`
- porta padrao `3000`
- banco H2 em `/metabase-data/metabase.db`
- volume `metabase_data`
- healthcheck HTTP

### Documentacao funcional

`metabase/README.md` documenta:

- setup inicial;
- criacao de usuario;
- conexao com Snowflake;
- configuracao de mapa do Brasil por GeoJSON;
- painel Xeno-Canto;
- matriz/visualizacao adicional.

Mudanca relevante observada:

- Foram adicionadas secoes de painel Xeno-Canto e matriz, com imagens `image-16.png`, `image-17.png` e `image-18.png`. Isso indica que a camada de BI evoluiu de setup/conexao para dashboards efetivos.

### Avaliacao tecnica do Metabase

Pontos fortes:

- Deploy simples.
- Volume persistente para manter configuracoes.
- Documentacao visual do setup e do dashboard.
- Uso previsto de role read-only no Snowflake.

Pontos de atencao:

- H2 e aceitavel para demonstracao, mas nao para producao.
- A imagem `latest` deve ser fixada para evitar atualizacoes inesperadas.
- Grants do Snowflake precisam apontar para o schema realmente materializado pelo dbt.

## 8. Fluxo integrado atualizado

```text
API Xeno-Canto
    |
    v
Airflow bronze
    - consulta por especie
    - per_page=500
    - salva query JSON
    - salva audio + metadata
    |
    v
S3 bronze
    - bronze/query
    - bronze/audio
    - bronze/metadata
    |
    v
Airflow silver
    - lista audio + metadata
    - divide em lotes de 10
    - retry por gravacao
    - registra erros em erros/silver
    - extrai features librosa
    |
    v
S3 silver
    - um Parquet por gravacao
    |
    v
Airflow gold via SSHOperator
    |
    v
dbt remoto em Docker
    - load_silver_features
    - run staging
    - run marts
    - unload gold
    |
    v
Snowflake
    - RAW.SRC_RECORDING_FEATURES
    - staging view
    - marts/gold tables
    |
    +--> Metabase dashboard
    |
    +--> S3 gold
             |
             v
        Airflow treinamento
```

## 9. Principais riscos tecnicos identificados

1. Divergencia entre schema dbt `gold` e schema Snowflake/Metabase `CORE`.
2. Divergencia local de paths `broze/metatada` versus paths das DAGs `bronze/metadata`.
3. `dbt_test` ainda comentado na DAG gold principal.
4. Uso de credenciais AWS diretamente em stages Snowflake.
5. Metabase usa H2 e imagem `latest`.
6. Dataset gold local com colunas `_COL_*`, exigindo logica posicional no treinamento.
7. Fallback de data invalida para data atual pode ocultar problemas de qualidade.
8. SSH entre droplets e montagem de chave no worker aumentam responsabilidade operacional.

## 10. Recomendacoes

1. Alinhar definitivamente o schema final: usar `GOLD` ou `CORE`, mas nao os dois.
2. Atualizar Snowflake grants para o mesmo schema materializado pelo dbt.
3. Reativar `dbt test` antes dos unloads na DAG gold.
4. Corrigir a estrutura local `s3/broze/metatada` para `s3/bronze/metadata`, caso ela seja usada como espelho do bucket.
5. Validar se `unload_fct_recordings.sql` esta gerando exatamente o schema esperado para ML.
6. Se possivel, exportar gold com nomes semanticos de colunas para reduzir dependencia de `_COL_*`.
7. Tornar `tamanho_lote` da DAG silver configuravel via Airflow Variable.
8. Registrar metricas de qualidade: quantidade de audios processados, falhas por lote, arquivos em `erros/silver`.
9. Substituir stages com AWS key/secret por Snowflake Storage Integration.
10. Fixar versao do Metabase no Docker Compose.
11. Considerar PostgreSQL como banco de aplicacao do Metabase se o uso for continuo.
12. Avaliar Git LFS ou remocao dos dados binarios grandes do repositorio.

## 11. Conclusao

O projeto evoluiu de uma estrutura inicial de pipeline para uma implementacao mais robusta, especialmente na camada silver. A DAG silver agora processa em lotes, possui retry por gravacao, registra falhas no S3 e limpa arquivos temporarios. A DAG bronze tambem foi ajustada para obter mais resultados por consulta e corrigiu a importacao de `AirflowException`.

No dbt, a carga silver ficou mais tolerante a datas invalidas, e o unload da fact passou a tentar gerar um dataset mais adequado para ML ao excluir colunas textuais. A amostra S3 tambem cresceu e agora mostra um conjunto mais realista: 15 audios/metadados, 15 Parquets silver e arquivos gold consolidados.

Os pontos mais importantes para estabilizacao continuam sendo consistencia de schemas e paths, ativacao dos testes dbt no fluxo principal, melhoria da gestao de credenciais Snowflake/S3 e reducao da dependencia de colunas `_COL_*` no treinamento.
