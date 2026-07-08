# DuckDB no Docker para DigitalOcean & dbt

O **DuckDB** é um banco de dados analítico baseado em coluna, de altíssimo desempenho, projetado para análises rápidas em arquivos locais ou em nuvem (como Parquet, CSV, etc.). 

Diferente de bancos tradicionais como PostgreSQL, o DuckDB **não possui um processo de servidor próprio** rodando de forma independente. Ele funciona de forma embarcada (in-process) dentro da própria aplicação (neste caso, o **dbt**).

Este diretório contém a estrutura e guias para rodar o DuckDB dentro do Docker na **DigitalOcean** integrado ao **dbt**.

---

## 🛠️ Arquiteturas Suportadas

Abaixo estão descritas as três principais formas de configurar o DuckDB no Docker para a DigitalOcean.

### Opção A: Volume Compartilhado (Banco de Dados Local no Droplet)
Ideal se você deseja que o arquivo `.duckdb` fique salvo fisicamente no disco do seu Droplet na DigitalOcean e possa ser acessado pelo container do dbt ou outros containers (como Airflow).

1. O container dbt e os outros serviços compartilham um volume chamado `duckdb_data`.
2. O arquivo de banco de dados é salvo como `/usr/app/data/prod.duckdb`.

**Como rodar:**
```bash
# Entre na pasta do duckdb
cd duckdb

# Suba o dbt-duckdb para verificar a conexão (ele usará o volume local por padrão)
docker compose up --build
```

---

### Opção B: Integração com AWS S3 (Data Lakehouse) - Recomendado ✨
Ao invés de salvar o banco de dados no disco do Droplet na DigitalOcean, o dbt roda o DuckDB de forma stateless (sem estado) e escreve/lê arquivos diretamente em um Bucket da **AWS S3** usando a extensão `httpfs`. Isso reduz drasticamente o consumo de armazenamento do Droplet e permite fácil integração de dados.

1. No seu `dbt/profiles.yml`, a conexão DuckDB já possui suporte a S3 e as extensões `httpfs` e `parquet` pré-configuradas.
2. Defina as credenciais da AWS nas variáveis de ambiente antes de executar o container.

**Como rodar com AWS S3:**
```bash
# Configure as credenciais da sua conta AWS
export AWS_ACCESS_KEY_ID="sua_access_key"
export AWS_SECRET_ACCESS_KEY="sua_secret_key"
export AWS_DEFAULT_REGION="us-east-1"  # região do seu bucket S3

# Suba o container injetando as credenciais
docker compose run --rm \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  -e AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION \
  dbt-duckdb dbt run --target dev_duckdb
```

Em seus modelos dbt, você pode ler arquivos do AWS S3 diretamente usando URLs no padrão `s3://`:
```sql
-- Exemplo de leitura direta de parquet no AWS S3
select * 
from read_parquet('s3://nome-do-seu-bucket-s3/caminho/usuarios.parquet')
```

*(Nota: Para utilizar serviços compatíveis como DigitalOcean Spaces, basta adicionalmente injetar a variável de ambiente `AWS_S3_ENDPOINT` apontando para o endpoint correspondente, ex: `nyc3.digitaloceanspaces.com`)*


---

### Opção C: Integração com MotherDuck (DuckDB Cloud-native)
Se você quer um banco de dados DuckDB compartilhado e acessível por dashboards locais ou por outros membros da equipe, a melhor solução é usar o **MotherDuck** (versão em nuvem e colaborativa do DuckDB).

1. Crie uma conta gratuita em [MotherDuck](https://motherduck.com).
2. Obtenha seu token de serviço.
3. Configure a variável `DBT_DUCKDB_PATH` apontando para `md:seu_nome_de_banco` e forneça o token na conexão.

**Como rodar com MotherDuck:**
```bash
docker compose run --rm \
  -e DBT_DUCKDB_PATH="md:my_database" \
  -e MOTHERDUCK_TOKEN="seu_token_aqui" \
  dbt-duckdb dbt run --target dev_duckdb
```

---

## 🚀 Passo a Passo: Deploy na DigitalOcean

Para implantar no seu Droplet da DigitalOcean:

### 1. Preparar o Droplet
Certifique-se de que o Docker e o Docker Compose estejam instalados no Droplet. Você pode usar uma imagem pronta da DigitalOcean com Docker (do Marketplace) ou instalar manualmente:
```bash
sudo apt update
sudo apt install docker.io docker-compose-v2 -y
```

### 2. Clonar o Repositório e Rodar o dbt
No terminal do seu Droplet:
```bash
# Clone seu repositório
git clone <url-do-repositorio>
cd pipelene-dados-pos-ia/duckdb

# Crie e execute o container para inicializar e depurar a conexão com o DuckDB
docker compose up --build
```

Se tudo estiver configurado corretamente, o comando do dbt irá compilar a imagem, inicializar o DuckDB (ou ler as configurações) e exibir uma mensagem de sucesso no `dbt debug`.

---

## 📁 Estrutura de Arquivos Criada
* [docker-compose.yml](file:///c:/Projetos/ia/pipelene-dados-pos-ia/duckdb/docker-compose.yml): Configuração de orquestração local/produção para rodar o dbt apontando para o DuckDB persistente.
* [profiles.yml](file:///c:/Projetos/ia/pipelene-dados-pos-ia/dbt/profiles.yml): Atualizado com a seção `dev_duckdb` dinâmica e suporte a extensões S3 e de leitura externa.
