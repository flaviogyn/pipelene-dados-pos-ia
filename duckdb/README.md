# DuckDB

Esta pasta contem um container utilitario para abrir DuckDB e consultar arquivos locais ou no S3. Ele nao substitui o dbt; o dbt fica na pasta `../dbt`.

DuckDB nao e um servidor de banco. Ele roda embarcado no processo que executa a consulta. Por isso, este container serve para acesso interativo, testes SQL e inspecao do arquivo `analytics.duckdb`.

## Enviando arquivos para o servidor

```bash
scp -r . root@<IP>:/root/duckdb
```

## Subir com Docker

Dentro da pasta `duckdb/`:

```bash
cp .env.example .env
docker compose build
docker compose run --rm duckdb
```

O arquivo principal fica em:

```text
./data/analytics.duckdb
```

Esse mesmo arquivo e montado pelo Compose da pasta `../dbt` em `/data/analytics.duckdb`.

## Executar SQL

Crie arquivos SQL em:

```text
duckdb/sql/
```

Depois rode:

```bash
docker compose run --rm duckdb -c "select 1"
```

Ou abra o modo interativo:

```bash
docker compose run --rm duckdb
```

## S3

Configure no `.env`:

```text
DUCKDB_PATH=/data/analytics.duckdb
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=us-east-1
AWS_S3_ENDPOINT=
AWS_S3_USE_SSL=true
```

Exemplo de consulta:

```sql
install httpfs;
load httpfs;

select *
from read_parquet('s3://nome-do-bucket/gold/*.parquet');
```

## Relacao com o projeto

Use esta pasta para:

- validar acesso ao DuckDB;
- testar consultas em Parquet;
- inspecionar tabelas criadas pelo dbt;
- apoiar o Metabase quando ele precisar ler o arquivo `.duckdb`.

Use a pasta `../dbt` para:

- executar `dbt debug`;
- executar `dbt run`;
- executar `dbt test`;
- gerar os modelos staging, dimensoes, fatos, features e predicoes.
