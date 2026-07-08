# dbt

Este diretório contém a estrutura do **dbt (data build tool)** configurada para rodar em containers Docker, conectando-se a:
* **PostgreSQL**
* **Snowflake**
* **Oracle 19c** (utilizando o driver Thin Mode do `python-oracledb`, sem necessidade de instalar a biblioteca Oracle Client).

---

## 🚀 Como Rodar com Docker

### 1. Construir a imagem Docker
No diretório `dbt/`, execute:
```bash
docker build -t pipelene-dbt .
```

### 2. Executar Comandos do dbt
Para executar comandos, mapeie o diretório do projeto e passe as variáveis de ambiente necessárias para conexão.

#### Exemplo: Conexão PostgreSQL (Target: `dev_postgres`)
```bash
docker run -it --rm `
  -v ${PWD}:/usr/app `
  -e DBT_POSTGRES_HOST="seu_host" `
  -e DBT_POSTGRES_PORT=5432 `
  -e DBT_POSTGRES_USER="postgres" `
  -e DBT_POSTGRES_PASSWORD="sua_senha" `
  -e DBT_POSTGRES_DBNAME="seu_banco" `
  -e DBT_POSTGRES_SCHEMA="public" `
  pipelene-dbt dbt debug --target dev_postgres
```

#### Exemplo: Conexão Snowflake (Target: `dev_snowflake`)
```bash
docker run -it --rm `
  -v ${PWD}:/usr/app `
  -e DBT_SNOWFLAKE_ACCOUNT="sua_conta" `
  -e DBT_SNOWFLAKE_USER="seu_usuario" `
  -e DBT_SNOWFLAKE_PASSWORD="sua_senha" `
  -e DBT_SNOWFLAKE_ROLE="sua_role" `
  -e DBT_SNOWFLAKE_DATABASE="seu_banco" `
  -e DBT_SNOWFLAKE_WAREHOUSE="seu_warehouse" `
  -e DBT_SNOWFLAKE_SCHEMA="seu_schema" `
  pipelene-dbt dbt debug --target dev_snowflake
```

#### Exemplo: Conexão Oracle (Target: `dev_oracle`)
```bash
docker run -it --rm `
  -v ${PWD}:/usr/app `
  -e DBT_ORACLE_HOST="seu_host" `
  -e DBT_ORACLE_PORT=1521 `
  -e DBT_ORACLE_USER="seu_usuario" `
  -e DBT_ORACLE_PASSWORD="sua_senha" `
  -e DBT_ORACLE_DBNAME="seu_servico_ou_sid" `
  -e DBT_ORACLE_SCHEMA="seu_schema" `
  pipelene-dbt dbt debug --target dev_oracle
```

---

## 📁 Estrutura de Arquivos Criada
* [Dockerfile](file:///c:/Projetos/ia/pipelene-dados-pos-ia/dbt/Dockerfile): Configura a imagem base com Python 3.10-slim e os adaptadores de conexão.
* [requirements.txt](file:///c:/Projetos/ia/pipelene-dados-pos-ia/dbt/requirements.txt): Declara o dbt-core e os adaptadores específicos.
* [profiles.yml](file:///c:/Projetos/ia/pipelene-dados-pos-ia/dbt/profiles.yml): Define os perfis de conexão dinamizados com variáveis de ambiente.
* [dbt_project.yml](file:///c:/Projetos/ia/pipelene-dados-pos-ia/dbt/dbt_project.yml): Configuração geral do projeto dbt.
* [models/](file:///c:/Projetos/ia/pipelene-dados-pos-ia/dbt/models): Modelos de exemplo para validação da estrutura.


