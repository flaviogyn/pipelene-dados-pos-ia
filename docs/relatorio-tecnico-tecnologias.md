# Relatorio tecnico do uso das tecnologias

Projeto: `pipelene-dados-pos-ia`

Escopo analisado: `airflow/`, `dbt/`, `docker/`, `metabase/`, `snowflake/` e `s3/`.

## 1. Visao geral da arquitetura

O projeto implementa um pipeline de dados e IA para gravacoes da base Xeno-Canto. A arquitetura segue uma divisao em camadas:

- `S3`: armazenamento em zonas bronze, silver e gold.
- `Airflow`: orquestracao dos processos de ingestao, tratamento, execucao dbt e treinamento de modelo.
- `dbt`: transformacoes analiticas sobre Snowflake, com modelos de staging e marts.
- `Snowflake`: data warehouse central, com stages para leitura/escrita no S3 e tabelas tipadas para dados processados.
- `Metabase`: camada de visualizacao e BI conectada ao Snowflake.
- `Docker`: empacotamento e padronizacao dos ambientes de Airflow, dbt e Metabase.

O fluxo principal pode ser descrito como:

1. Airflow consulta a API Xeno-Canto e grava arquivos brutos no S3 bronze.
2. Airflow processa audios com `librosa`, extrai features acusticas e grava Parquet no S3 silver.
3. Airflow aciona remotamente o dbt via SSH.
4. dbt executa macro de carga da silver para o Snowflake RAW.
5. dbt cria uma view de staging e tabelas gold.
6. dbt exporta tabelas gold de volta para S3.
7. Metabase consome o Snowflake para dashboards e analises.
8. Airflow tambem possui DAG de treinamento de modelos usando dados gold.

## 2. Airflow

### Papel no projeto

O Airflow e usado como orquestrador do pipeline. Ele coordena ingestao, processamento, transformacao e treinamento. A configuracao em `airflow/docker-compose.yml` utiliza Airflow 3.1.1 com `CeleryExecutor`, PostgreSQL como metastore e Redis como broker.

### Componentes Docker

O compose define os seguintes servicos:

- `postgres`: banco de metadados do Airflow.
- `redis`: broker do Celery.
- `airflow-apiserver`: interface/API do Airflow na porta 8080.
- `airflow-scheduler`: agendamento e controle das DAGs.
- `airflow-worker`: execucao distribuida das tasks Celery.
- `airflow-triggerer`: suporte a execucoes assicronas.
- `airflow-dag-processor`: processamento de DAGs.
- `airflow-init`: inicializacao, migracao do banco e criacao do usuario.
- `flower`: monitoramento opcional do Celery.

A imagem e customizada em `airflow/Dockerfile`, partindo de `apache/airflow:3.1.1` e instalando dependencias listadas em `airflow/requirements.txt`.

### Dependencias usadas

O Airflow inclui bibliotecas de processamento e ML:

- `pandas`: manipulacao tabular.
- `librosa` e `soundfile`: leitura de audio e extracao de features.
- `pyarrow`: escrita de arquivos Parquet.
- `numpy`: calculos numericos.
- `tensorflow` e `scikit-learn`: treinamento de modelos.

### DAGs principais

`xeno_canto_bronze_dag.py`

Responsavel pela ingestao bronze. A DAG le configuracoes do Airflow Variables, consulta a API Xeno-Canto, grava JSON de query, baixa audios e salva metadados individuais no S3. Ela usa `S3Hook` com a connection `aws_conn` e aplica Dynamic Task Mapping para paralelizar consultas e downloads.

`xeno_canto_silver_dag.py`

Responsavel pela camada silver. A DAG lista audios e metadados na bronze, baixa os audios para `/tmp`, extrai features acusticas com `librosa` e salva um Parquet por gravacao em `silver/<id>.parquet`. As features incluem duracao, energia media, RMS, zero crossing rate, centroide espectral, largura espectral e MFCCs 1 a 20 com media e desvio padrao.

`xeno_canto_gold_dag.py`

Orquestra a etapa analitica. Em vez de rodar dbt no mesmo container, usa `SSHOperator` para acessar uma VPS/droplet de dbt e executar comandos Docker remotos. O fluxo definido e:

1. `dbt run-operation load_silver_features`
2. `dbt run --select staging`
3. `dbt run --select marts`
4. `dbt run-operation unload_fct_recordings`
5. `dbt run-operation unload_dim_species`

O teste dbt aparece comentado no arquivo, indicando uma decisao pendente ou temporaria.

`run_dbt_snowflake.py`

DAG simples para executar `dbt run` e `dbt test` remotamente via SSH.

`xeno_canto_treinamento.py`

Executa treinamento usando o dataset `gold/fct_recordings.parquet` no S3. Ha duas abordagens:

- modelo com TensorFlow/Keras;
- rede neural implementada manualmente com NumPy.

Os artefatos sao salvos no S3 em `treinamentos/modelo.keras`, `treinamentos/modelo_hardcode.npz` e `treinamentos/resultados.json`.

### Avaliacao tecnica

Pontos fortes:

- Boa separacao das etapas bronze, silver, gold e treinamento.
- Uso adequado de Dynamic Task Mapping para paralelizar downloads/processamentos.
- Uso de Airflow Variables e Connections para parametrizacao.
- Retentativas no download de audio, com backoff exponencial.
- Isolamento do dbt em um ambiente remoto, reduzindo acoplamento operacional.

Pontos de atencao:

- Em `xeno_canto_bronze_dag.py` e `xeno_canto_silver_dag.py` ha mensagens de erro citando `s3_bucket`, mas a variavel usada e `S3_Bucket`; isso pode confundir suporte operacional.
- `AirflowException` e usada no bronze, mas nao aparece importada no arquivo.
- A DAG gold esta com `dbt_test` comentado, entao o fluxo gold atual pode publicar dados sem validacao automatica.
- A DAG de treinamento usa nomes `_COL_16`, `_COL_17...`, o que sugere dependencia de um schema de Parquet lido sem nomes de colunas esperados. Isso reduz robustez.
- O worker monta uma chave SSH local, exigindo cuidado com permissoes e rotacao de credenciais.

## 3. dbt

### Papel no projeto

O dbt e usado para transformar os dados carregados no Snowflake em estruturas analiticas. O projeto esta em `dbt/dbt_project.yml` com profile `pipelene_dbt`.

### Configuracao

O target principal e `dev_snowflake`, definido em `dbt/profiles.yml`. As credenciais sao carregadas por variaveis de ambiente:

- `DBT_SNOWFLAKE_ACCOUNT`
- `DBT_SNOWFLAKE_USER`
- `DBT_SNOWFLAKE_PASSWORD`
- `DBT_SNOWFLAKE_ROLE`
- `DBT_SNOWFLAKE_DATABASE`
- `DBT_SNOWFLAKE_WAREHOUSE`
- `DBT_SNOWFLAKE_SCHEMA`

O projeto tambem mantem adaptadores PostgreSQL e Oracle em `requirements.txt`, mas o fluxo principal observado usa Snowflake.

### Estrutura de modelos

`models/staging`

- `sources.yml`: define a source `xeno_raw` no database `XENO_DB`, schema `RAW`, tabela `src_recording_features`.
- `stg_recording_features.sql`: cria uma view de staging a partir da tabela RAW, renomeando e normalizando campos para nomes analiticos.
- `stg_recording_features.yml`: documenta e testa campos como `recording_id` e `mfcc_1_mean`.

`models/marts`

- `dim_species.sql`: dimensao de especies, com chave surrogate via `dbt_utils.generate_surrogate_key` e flag `is_target_species`.
- `fct_recordings.sql`: fato principal, uma linha por gravacao, com metadados, coordenadas, dados de qualidade, features acusticas e label binario.
- `marts.yml`: testes de unicidade, `not_null` e valores aceitos para `quality_rating`.

### Macros

`load_silver_features.sql`

Executa um `MERGE` da silver no Snowflake. A macro le Parquet do stage `XENO_DB.RAW.S3_STAGE_SILVER` e atualiza/insere registros em `XENO_DB.RAW.SRC_RECORDING_FEATURES`. A escolha por `MERGE` torna a carga idempotente para reprocessamentos por `id`.

`unload_fct_recordings.sql` e `unload_dim_species.sql`

Exportam as tabelas gold para S3 usando `COPY INTO`, formato Parquet, `SINGLE = TRUE` e `OVERWRITE = TRUE`. Isso gera arquivos de saida estaveis em:

- `gold/fct_recordings.parquet`
- `gold/dim_species.parquet`

### Pacotes

O projeto usa `dbt_utils`, definido em `packages.yml`, principalmente para gerar chaves surrogate.

### Avaliacao tecnica

Pontos fortes:

- Separacao clara entre staging e marts.
- Uso de testes dbt em chaves e campos criticos.
- Carga silver implementada como `MERGE`, adequada para reprocessamento.
- Tabelas gold materializadas como `table`, apropriadas para consumo pelo Metabase.
- Variaveis `target_genus` e `target_species` tornam a especie-alvo configuravel.

Pontos de atencao:

- `dbt_project.yml` define marts no schema `gold`, enquanto o script Snowflake cria `XENO_DB.CORE` como gold e concede acesso ao Metabase em `CORE`. Ha divergencia entre `gold` e `CORE`.
- `sources.yml` fixa `database: XENO_DB`, reduzindo portabilidade entre ambientes.
- O compose do dbt define `DBT_TARGET` default como `dev_snowflake`, mas `.env.example` usa `DBT_TARGET=dev`; isso pode causar erro se o usuario seguir o exemplo literalmente.
- O teste dbt existe, mas nao esta ativo na DAG gold principal.

## 4. Docker

### Papel no projeto

Docker e usado como camada de reproducibilidade e deploy. O projeto possui configuracoes Docker separadas para Airflow, dbt e Metabase, alem de um script de instalacao para VPS Ubuntu.

### Airflow

O Airflow usa um compose completo com multiplos servicos e uma imagem customizada. Isso permite instalar bibliotecas pesadas de audio e ML sem depender de instalacao manual na VPS.

### dbt

O dbt usa `python:3.10-slim`, instala dependencias do `requirements.txt`, define `DBT_PROFILES_DIR=/usr/app` e monta:

- `./` em `/usr/app`
- `../s3` em `/workspace/s3`

O entrypoint e `dbt`, facilitando execucoes como:

- `docker compose run --rm dbt debug`
- `docker compose run --rm dbt run`
- `docker compose run --rm dbt test`
- `docker compose run --rm dbt run-operation <macro>`

### Metabase

O Metabase usa a imagem `metabase/metabase:latest`, porta configuravel por `MB_PORT` e volume persistente `metabase_data`.

### Script de instalacao

`docker/install-docker-ubuntu.sh` automatiza instalacao do Docker Engine, CLI, containerd, Buildx e Compose Plugin em Ubuntu. Tambem habilita o servico Docker, adiciona usuario ao grupo `docker` quando aplicavel e libera SSH no UFW.

### Avaliacao tecnica

Pontos fortes:

- Ambientes isolados por responsabilidade.
- Compose do Airflow bem completo para execucao distribuida.
- Compose do dbt simples e adequado para runner de comandos.
- Metabase com persistencia local via volume.

Pontos de atencao:

- `metabase/metabase:latest` nao fixa versao, o que pode gerar mudancas inesperadas em deploys futuros.
- Airflow usa TensorFlow dentro da imagem; isso pode aumentar bastante tempo de build e consumo de memoria.
- O uso de VPS separadas com SSH exige documentacao operacional rigorosa para chaves, firewall e paths.

## 5. Snowflake

### Papel no projeto

Snowflake e o data warehouse do projeto. Ele armazena a tabela RAW carregada da silver e recebe as transformacoes dbt para staging/gold.

### Objetos criados

O script `snowflake/setup_snowflake_xeno.sql` cria:

- Warehouse `XENO_WH`
- Database `XENO_DB`
- Schemas `RAW`, `STAGING` e `CORE`
- Stage `XENO_DB.RAW.S3_STAGE_SILVER`
- Stage `XENO_DB.RAW.S3_STAGE_GOLD`
- Tabela `XENO_DB.RAW.SRC_RECORDING_FEATURES`
- Role `METABASE_RO`
- Usuario de servico `METABASE_SVC`

### Integracao com S3

O Snowflake acessa o S3 via external stages:

- `S3_STAGE_SILVER`: le arquivos Parquet em `s3://xeno-canto-s3/silver/`
- `S3_STAGE_GOLD`: exporta tabelas para `s3://xeno-canto-s3/gold/`

A tabela `SRC_RECORDING_FEATURES` modela os Parquets da silver com colunas tipadas, evitando depender de uma coluna semiestruturada `VARIANT`.

### Seguranca e acesso

O script cria uma role read-only para o Metabase, restringindo acesso ao schema definido como gold no script. A intencao esta correta: BI deve consultar apenas tabelas finais, nao RAW nem STAGING.

### Avaliacao tecnica

Pontos fortes:

- Warehouse pequeno com auto suspend, adequado para controle de custo.
- Stages separados para leitura silver e exportacao gold.
- Tabela RAW fortemente tipada.
- Usuario/role dedicados para Metabase.

Pontos de atencao:

- O script usa placeholders de credenciais AWS diretamente no SQL. Em producao, o ideal e usar Storage Integration do Snowflake, evitando chaves estaticas em scripts.
- Ha divergencia entre schema gold no dbt (`gold`) e schema de permissao no Snowflake (`CORE`).
- Comentario do warehouse diz "60s", mas `AUTO_SUSPEND = 120`; alinhar comentario e configuracao.

## 6. S3

### Papel no projeto

O S3 funciona como data lake do pipeline. O diretorio `s3/` simula localmente a estrutura do bucket e contem exemplos reais de arquivos.

### Estrutura observada

Camada bronze:

- `s3/broze/audio/`: audio MP3 bruto.
- `s3/broze/metatada/`: JSONs de metadados e consultas.

Camada silver:

- `s3/silver/<id>.parquet`: Parquets por gravacao com metadados e features acusticas.
- `s3/silver/audio_features.parquet` e `s3/silver/recordings.parquet`: arquivos presentes, mas com tamanho 0 no workspace.

Camada gold:

- `s3/gold/dim_species.parquet`
- `s3/gold/fct_recordings.parquet`

### Uso no pipeline

O Airflow grava bronze e silver diretamente no S3 via `S3Hook`. O Snowflake le silver por stage e o dbt exporta gold de volta para S3 via macros de unload. A DAG de treinamento consome `gold/fct_recordings.parquet`.

### Avaliacao tecnica

Pontos fortes:

- Camadas de dados bem definidas.
- Uso de Parquet na silver e gold, adequado para eficiencia e tipagem.
- Gold exportada com nomes estaveis para consumo posterior.

Pontos de atencao:

- Ha nomes de diretorios com erros de digitacao no workspace: `broze` e `metatada`. As DAGs usam `bronze` e `metadata`; isso indica divergencia entre simulacao local e paths esperados no bucket real.
- Alguns arquivos silver locais estao vazios, o que pode quebrar testes ou leituras se forem usados como amostra.
- O repositorio contem arquivos de dados e audio; dependendo do objetivo, pode ser melhor manter apenas amostras pequenas ou usar Git LFS.

## 7. Metabase

### Papel no projeto

Metabase e usado como ferramenta de BI e visualizacao. Ele deve se conectar ao Snowflake usando o usuario de servico `METABASE_SVC` e role `METABASE_RO`.

### Configuracao Docker

O compose em `metabase/docker-compose.yml` sobe um container unico:

- imagem `metabase/metabase:latest`
- porta `3000` por padrao
- banco interno H2
- volume `metabase_data` para persistencia
- healthcheck em `/api/health`

### Configuracao funcional

O README documenta:

- setup inicial do Metabase;
- criacao de usuario;
- conexao com Snowflake;
- configuracao de mapa customizado do Brasil via GeoJSON.

### Avaliacao tecnica

Pontos fortes:

- Deploy simples e independente.
- Persistencia configurada por volume Docker.
- Uso de role read-only no Snowflake, reduzindo risco de acesso indevido.

Pontos de atencao:

- H2 e simples para projeto academico ou demonstracao, mas nao e ideal para producao. Para uso continuo, recomenda-se PostgreSQL como banco de aplicacao do Metabase.
- Imagem `latest` deve ser fixada em uma versao para evitar atualizacoes inesperadas.
- Permissoes no Snowflake precisam ser alinhadas ao schema real usado pelo dbt.

## 8. Fluxo tecnico integrado

```text
API Xeno-Canto
    |
    v
Airflow bronze DAG
    |
    v
S3 bronze: audio + metadata JSON
    |
    v
Airflow silver DAG com librosa
    |
    v
S3 silver: Parquet com metadados + features acusticas
    |
    v
Snowflake stage + dbt macro load_silver_features
    |
    v
Snowflake RAW.SRC_RECORDING_FEATURES
    |
    v
dbt staging view
    |
    v
dbt gold tables: dim_species + fct_recordings
    |
    +--> Metabase / BI
    |
    +--> dbt unload para S3 gold
             |
             v
        Airflow treinamento / modelos IA
```

## 9. Principais riscos tecnicos identificados

1. Divergencia de schemas: dbt usa `gold`, Snowflake setup usa `CORE` para gold/permissoes.
2. Divergencia de paths S3: workspace local usa `broze/metatada`, DAGs usam `bronze/metadata`.
3. Teste dbt comentado na DAG gold, reduzindo garantia antes de publicar gold.
4. Uso de credenciais AWS em stage Snowflake por chave estatica, em vez de Storage Integration.
5. Metabase com banco H2 e imagem `latest`, adequado para demonstracao, mas fragil para producao.
6. Dependencia de SSH entre droplets para acionar dbt, exigindo monitoramento de chave, firewall, path remoto e disponibilidade da VPS.
7. Colunas `_COL_*` na DAG de treinamento sugerem risco de schema instavel para ML.

## 10. Recomendacoes

1. Alinhar nomenclatura de schemas: escolher `GOLD` ou `CORE` e refletir isso no Snowflake, dbt e grants do Metabase.
2. Corrigir paths locais/documentados de S3 para `bronze/metadata`, mantendo consistencia com as DAGs.
3. Reativar `dbt test` na DAG gold antes dos unloads.
4. Substituir credenciais AWS estaticas em stages por Snowflake Storage Integration.
5. Fixar versoes Docker, especialmente Metabase.
6. Trocar H2 por PostgreSQL se Metabase for usado continuamente.
7. Padronizar schema do Parquet gold para evitar dependencia de `_COL_*` no treinamento.
8. Adicionar testes automatizados para macros dbt, DAG parsing e validacao de schema dos Parquets.
9. Documentar runbook operacional: variaveis Airflow, connections, secrets, chaves SSH, buckets, stages e comandos de recuperacao.

## 11. Conclusao

O projeto apresenta uma arquitetura coerente de pipeline moderno de dados: ingestao com Airflow, armazenamento em S3, processamento analitico com Snowflake/dbt, consumo em Metabase e uma etapa de IA usando os dados gold. A separacao em camadas bronze, silver e gold esta bem representada no codigo.

Os principais ajustes tecnicos recomendados sao de consistencia operacional e robustez: alinhar nomes de schemas e paths, ativar testes no fluxo principal, fortalecer gestao de credenciais e estabilizar versoes Docker. Com esses ajustes, a solucao fica mais previsivel, auditavel e preparada para evoluir alem de uma demonstracao academica.
