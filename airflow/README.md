# Instalação do Airflow em uma droplet da DigitalOcean (usuário root)

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

