# Metabase no Docker

Este diretório prepara o Metabase para rodar com Docker usando o banco interno H2 do próprio Metabase.

## Requisitos

- Docker
- Docker Compose

## Estrutura

```text
metabase/
├── docker-compose.yml
├── .env.example
└── README.md
```

## Como usar

1. Crie o arquivo de ambiente a partir do exemplo:

```bash
cp .env.example .env
```

2. Suba o container:

```bash
docker compose up -d
```

3. Acesse o Metabase em:

```text
http://localhost:3000
```

4. Para parar o container:

```bash
docker compose down
```

## Armazenamento do Metabase

O compose usa um volume persistente para guardar o banco H2 do Metabase em:

```text
/metabase-data/metabase.db
```

Isso evita perder os dados do Metabase entre reinicializações do container.

## Observações sobre o DuckDB

O compose também monta a pasta do projeto:

```text
../duckdb/data:/data/duckdb:ro
```

Isso permite que o Metabase tenha acesso local aos arquivos do DuckDB, como por exemplo:

```text
../duckdb/data/analytics.duckdb
```

## Variáveis de ambiente

O arquivo `.env` pode conter apenas:

```env
MB_PORT=3000
```
