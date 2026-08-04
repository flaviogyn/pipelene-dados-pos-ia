# Instalação do Airflow em uma droplet da DigitalOcean (usuário root)

## IP Cloud

```text
142.93.194.1
```

Este arquivo reúne os passos para subir o Airflow usando o arquivo de compose já disponibilizado neste projeto. Não é necessário alterar o arquivo docker-compose.yml.

## 1. Atualizar o sistema

```bash
apt update && apt upgrade -y
```

## 2. Instalar Docker e Docker Compose

```bash
apt install -y ca-certificates curl gnupg lsb-release
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" > /etc/apt/sources.list.d/docker.list

apt update
apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
systemctl enable --now docker
```

## 3. Entrar no diretório do projeto

```bash
cd /root/pipelene-dados-pos-ia/airflow
```

## 4. Criar diretórios necessários

```bash
mkdir -p dags logs plugins config
```

## 5. Criar o arquivo .env

Defina o usuário do container como root e informe os segredos do Airflow.

```bash
cat > .env <<'EOF'
AIRFLOW_UID=0
AIRFLOW__API_AUTH__JWT_SECRET=troque-por-um-segredo-forte
AIRFLOW__CORE__FERNET_KEY=troque-por-uma-chave-forte
_PIP_ADDITIONAL_REQUIREMENTS=
EOF
```

> Se preferir, gere uma chave Fernet com Python e substitua o valor acima.

## 6. Inicializar o Airflow

```bash
docker compose up airflow-init
```

## 7. Subir os serviços

```bash
docker compose up -d
```

## 8. Verificar os containers

```bash
docker ps
```

## 9. Acessar a interface web

A interface do Airflow ficará disponível em:

```bash
http://SEU_IP_PUBLICO:8080
```

Usuário e senha padrão:

```text
airflow
airflow
```

## 10. Verificar o funcionamento

```bash
docker compose logs -f airflow-scheduler
```

Se quiser testar o worker manualmente:

```bash
docker compose run --rm airflow-worker airflow info
```

## 11. Criando SSH entre as VPS

1. SSH entre os droplets — o Airflow precisa conseguir logar no droplet do dbt via chave SSH, do mesmo jeito que você já fez do seu computador local pro Metabase, só que agora é droplet-pra-droplet:
bash# gerar uma chave dedicada no droplet do Airflow (não reuse sua chave pessoal)

```bash
ssh-keygen -t ed25519 -f ~/.ssh/airflow_to_dbt -N ""
```

Copiar a chave pública para o droplet do dbt

```bash
ssh-copy-id -i ~/.ssh/airflow_to_dbt.pub root@IP_DO_DROPLET_B
```

2. Cloud Firewall — libere a porta 22 no droplet B, mas restrinja a origem ao IP do droplet do Airflow (não 0.0.0.0/0). Isso é a mesma pegadinha que vocês já pegaram com o Metabase: Cloud Firewall da DigitalOcean é separado do UFW, então precisa configurar os dois (ou pelo menos o Cloud Firewall).

Dica: se os dois droplets estiverem na mesma região, ative uma VPC entre eles — tráfego fica na rede privada (mais seguro, sem custo de banda, e você pode restringir o SSH só à rede interna).

3. Provider SSH no Airflow — instale o pacote (se ainda não tiver):

```bash
bashpip install apache-airflow-providers-ssh
```

4. Connection no Airflow — Admin → Connections → nova conexão tipo SSH:

Host: IP do droplet B
Username: o usuário SSH
Extra: {"key_file": "/caminho/da/chave/airflow_to_dbt"} (ou cole a chave privada direto, se preferir)

5. Task no DAG — usar SSHOperator chamando o docker compose run remotamente:

```bash
pythonfrom airflow.providers.ssh.operators.ssh import SSHOperator
```

```bash
run_dbt = SSHOperator(
    task_id="run_dbt",
    ssh_conn_id="dbt_remote",
    command=(
        "cd /caminho/do/projeto && "
        "docker compose run --rm dbt run --target dev_postgres && "
        "docker compose run --rm dbt test --target dev_postgres"
    ),
    cmd_timeout=600,
)
```

A vantagem de fazer assim: o Airflow nem precisa saber que existe DuckDB por trás — ele só dispara um comando remoto e espera o código de saída. Toda a complexidade de lock/concorrência que já resolvemos (rename atômico, --readonly) continua isolada dentro do droplet B.