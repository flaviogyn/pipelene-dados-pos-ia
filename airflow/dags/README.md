# Guia de configuração do Airflow DAG

## 1. Configurar autenticação SSH entre droplets
O Airflow precisa se conectar ao droplet do dbt/DuckDB via SSH usando chave pública/privada.

No droplet do Airflow, gere uma chave dedicada (não use sua chave pessoal):

```bash
ssh-keygen -t ed25519 -f ~/.ssh/airflow_to_dbt -N ""
```

Copie a chave pública para o droplet do dbt/DuckDB:

```bash
ssh-copy-id -i ~/.ssh/airflow_to_dbt.pub root@IP_DO_DROPLET_B
```

> Se você estiver usando outro usuário SSH, substitua `root` pelo usuário correto.

## 2. Ajustar firewall e rede

- No droplet do dbt/DuckDB, libere a porta `22` apenas para o IP do droplet do Airflow. Não use `0.0.0.0/0`.
- Na DigitalOcean, o Cloud Firewall é separado do UFW. Configure o Cloud Firewall e, se necessário, também o UFW no droplet.

### Dica
Se os droplets estiverem na mesma região, crie uma VPC entre eles. Assim o tráfego circula pela rede privada, fica mais seguro e não gera custo de banda pública.

## 3. Instalar o provider SSH no Airflow
No ambiente do Airflow, instale o pacote do provider SSH:

```bash
pip install apache-airflow-providers-ssh
```

## 4. Criar a conexão SSH no Airflow
No Airflow, vá em `Admin → Connections` e crie uma nova conexão do tipo `SSH`.

- Host: IP do droplet do dbt/DuckDB
- Username: usuário SSH
- Extra:

```json
{"key_file": "/caminho/da/chave/airflow_to_dbt"}
```

> Alternativa: cole a chave privada diretamente em `Extra` se preferir.

## 5. Configurar a task no DAG
Use `SSHOperator` para executar o `docker compose run` remotamente no droplet do dbt/DuckDB.

O script principal de execução é:

```text
run_dbt_duckdb.py
```
