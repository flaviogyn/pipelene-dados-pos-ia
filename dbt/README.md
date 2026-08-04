# dbt

## IP Cloud

```text
159.89.226.24
```

Projeto dbt configurado para usar Snowflake como target de desenvolvimento.

O target principal é `dev_snowflake`. Ele usa Snowflake como destino e pode ler/escrever dados em storage compatível com S3.

## Enviando arquivos para o servidor

```bash
scp -r . root@<IP>:/root/dbt
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
../s3           -> /workspace/s3
```

O dbt acessa seus arquivos do projeto e o S3 via variáveis de ambiente.

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
- `profiles.yml`: target `dev_snowflake`.
- `docker-compose.yml`: runner Docker do dbt.
