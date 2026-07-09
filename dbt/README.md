# dbt

Projeto dbt configurado para usar DuckDB como substituto analitico do Snowflake.

O target principal e `dev_duckdb`. Ele usa DuckDB localmente no container e pode ler/escrever dados no AWS S3 ou em storage compativel com S3.

## Subir com Docker

Dentro da pasta `dbt/`:

```bash
cp .env.example .env
docker compose build
docker compose run --rm dbt debug --target dev_duckdb
```

Executar os modelos:

```bash
docker compose run --rm dbt run --target dev_duckdb
docker compose run --rm dbt test --target dev_duckdb
```

Gerar documentacao:

```bash
docker compose run --rm dbt docs generate --target dev_duckdb
```

## Variaveis principais

Configure no `.env`:

```text
DBT_DUCKDB_PATH=/data/analytics.duckdb
DBT_DUCKDB_THREADS=4
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=us-east-1
AWS_S3_ENDPOINT=
AWS_S3_USE_SSL=true
```

Para AWS S3 puro, deixe `AWS_S3_ENDPOINT` vazio. Para DigitalOcean Spaces, use o endpoint do seu espaco, por exemplo:

```text
AWS_S3_ENDPOINT=nyc3.digitaloceanspaces.com
```

## Volumes

O Compose monta:

```text
./              -> /usr/app
../duckdb/data  -> /data
../s3           -> /workspace/s3
```

Assim, o arquivo DuckDB compartilhado fica em:

```text
../duckdb/data/analytics.duckdb
```

## Como usar S3 nos modelos

Exemplo de leitura de Parquet:

```sql
select *
from read_parquet('s3://nome-do-bucket/bronze/usuarios/*.parquet');
```

Exemplo de escrita em Parquet:

```sql
copy (
    select *
    from {{ ref('fct_usuarios_ativos') }}
) to 's3://nome-do-bucket/gold/fct_usuarios_ativos.parquet'
  (format parquet);
```

## Estrutura esperada no projeto

- `models/staging`: modelos de staging.
- `models/marts/dim`: dimensoes.
- `models/marts/fct`: fatos.
- `models/ml`: features, metricas e predicoes para ML.
- `profiles.yml`: target `dev_duckdb`.
- `docker-compose.yml`: runner Docker do dbt.
