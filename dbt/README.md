# dbt

## IP Cloud

```text
142.93.205.113
```

Projeto dbt configurado para usar Snowflake como target de desenvolvimento.

O target principal e `dev_snowflake`. Ele usa Snowflake como destino das transformacoes dbt.

## Enviando arquivos para o servidor

```bash
scp -r * root@<IP>:/root/dbt
scp -r .* root@<IP>:/root/dbt
```

## Subir com Docker

Dentro da pasta `dbt/`:

```bash
cp .env.example .env
docker compose build
docker compose run --rm dbt debug --target dev_snowflake
```

Executar os modelos:

```bash
docker compose run --rm dbt run --target dev_snowflake
docker compose run --rm dbt test --target dev_snowflake
```

Gerar documentacao:

```bash
docker compose run --rm dbt docs generate --target dev_snowflake
```

## Variaveis principais

Configure no `.env`:

```text
DBT_TARGET=dev_snowflake
DBT_SNOWFLAKE_ACCOUNT=<sua_conta>
DBT_SNOWFLAKE_USER=<seu_usuario>
DBT_SNOWFLAKE_PASSWORD=<sua_senha>
DBT_SNOWFLAKE_ROLE=<seu_papel>
DBT_SNOWFLAKE_DATABASE=<seu_banco>
DBT_SNOWFLAKE_WAREHOUSE=<seu_warehouse>
DBT_SNOWFLAKE_SCHEMA=<seu_esquema>
```

## Arquivos externos no Snowflake

Para ler arquivos Parquet externos, configure um stage e um file format no Snowflake. Exemplo:

```sql
create or replace file format parquet_format
  type = parquet;

create or replace stage raw_parquet_stage
  url = 's3://nome-do-bucket/bronze/'
  file_format = parquet_format;
```

## Volumes

O Compose monta:

```text
./              -> /usr/app
../s3           -> /workspace/s3
```

O dbt acessa os modelos do projeto e executa as transformacoes no Snowflake.

Exemplo de leitura de Parquet em um modelo:

```sql
select *
from @raw_parquet_stage/usuarios/
  (file_format => parquet_format);
```

Exemplo de escrita em tabela Snowflake:

```sql
create or replace table gold.fct_usuarios_ativos as
select *
from {{ ref('fct_usuarios_ativos') }};
```

## Estrutura esperada no projeto

- `models/staging`: modelos de staging.
- `models/marts/dim`: dimensoes.
- `models/marts/fct`: fatos.
- `models/ml`: features, metricas e predicoes para ML.
- `profiles.yml`: target `dev_snowflake`.
- `docker-compose.yml`: runner Docker do dbt.

## Atualizar arquivos do Docker (parar e subir novamente)

```bash
docker compose down
docker compose up -d --build
```

## ⚙️ Configuração do Ambiente Local (.env)

Para executar o projeto localmente, crie um arquivo `.env` na raiz do repositório utilizando o arquivo `.env.example` como base.

```bash
cp .env.example .env
```

Preencha as variáveis de ambiente seguindo as orientações abaixo:

### 1. Snowflake & dbt
Para obter ou validar as credenciais do Snowflake, acesse a página de autenticação do console:
🔗 [Configurações de Autenticação - Snowflake](https://app.snowflake.com/sfedu02/gfb24387/settings/authentication)

* `DBT_TARGET`: Defina como `dev` para desenvolvimento local.
* `DBT_SNOWFLAKE_ACCOUNT`: O identificador da conta. Baseado na URL do projeto, use o formato `orgname-accountname`. Para este ambiente, preencha com: **`sfedu02-gfb24387`**.
* `DBT_SNOWFLAKE_USER`: Seu usuário individual de acesso ao Snowflake.
* `DBT_SNOWFLAKE_PASSWORD`: Sua senha pessoal do Snowflake.
* `DBT_SNOWFLAKE_ROLE`: A Role que o dbt utilizará (ex: `TRANSFORMER` ou `SYSADMIN`).
* `DBT_SNOWFLAKE_DATABASE`: O nome do banco de dados de desenvolvimento.
* `DBT_SNOWFLAKE_WAREHOUSE`: O Warehouse utilizado para computação (ex: `COMPUTE_WH`).
* `DBT_SNOWFLAKE_SCHEMA`: Seu schema de trabalho (geralmente segue o padrão `dbt_seuusuario`).

### 2. AWS & S3 Storage
Utilizado para integração e carregamento de dados (stages).

* `AWS_ACCESS_KEY_ID`: Sua chave de acesso pública da AWS.
* `AWS_SECRET_ACCESS_KEY`: Sua chave de acesso privada da AWS.
* `AWS_DEFAULT_REGION`: Região padrão do bucket (ex: `us-east-1`).
* `AWS_S3_ENDPOINT`: Mantenha `https://amazonaws.com` para a AWS oficial. Se estiver simulando localmente via LocalStack, mude para `http://localhost:4566`.
* `AWS_S3_USE_SSL`: Mantenha `true` para produção/AWS real.
* `AWS_SESSION_TOKEN`: Preencha **apenas** se você utiliza credenciais temporárias via MFA ou AWS STS. Pode deixar em branco se usar chaves de longa duração.

⚠️ **IMPORTANTE:** Nunca comite o arquivo `.env` modificado. Ele já está listado no `.gitignore` para garantir a segurança das credenciais.

### Dependencias de packages Python

```bash
docker compose run --rm dbt deps
```

Ele cria/atualiza a pasta dbt_packages/ dentro do projeto com o dbt_utils.
Se o Airflow chama o dbt remotamente via SSH, depois disso a DAG já deve conseguir executar o modelo que usa dbt_utils.