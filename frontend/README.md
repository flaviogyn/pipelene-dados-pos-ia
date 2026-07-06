# 🐍 DAG Manager — DigitalOcean Droplet

Interface web para **upload, listagem e exclusão** de arquivos Python (`.py`) na pasta `/root/dags` de um droplet DigitalOcean rodando Apache Airflow.

---

## 📋 Sumário

- [Visão Geral](#-visão-geral)
- [Funcionalidades](#-funcionalidades)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Pré-requisitos](#-pré-requisitos)
- [Variáveis de Ambiente](#-variáveis-de-ambiente)
- [Rodando Localmente](#-rodando-localmente)
  - [Com Docker (recomendado)](#opção-1--com-docker-recomendado)
  - [Com Python direto](#opção-2--com-python-direto)
- [Deploy no DigitalOcean](#-deploy-no-digitalocean)
- [Segurança](#-segurança)

---

## 🔍 Visão Geral

O **DAG Manager** é uma aplicação Flask de página única que permite gerenciar os arquivos de DAG do Airflow diretamente pelo navegador, sem precisar de acesso SSH ao servidor. É empacotado em um container Docker com **Gunicorn** como servidor de produção.

---

## ✨ Funcionalidades

| Recurso | Descrição |
|---|---|
| 📤 Upload drag & drop | Arraste ou selecione arquivos `.py` para envio |
| 📊 Barra de progresso | Acompanhe o upload em tempo real via XHR |
| 📁 Listagem de arquivos | Exibe nome, tamanho e data de modificação |
| ⬇ Download | Baixe qualquer DAG com um clique |
| 🗑 Exclusão segura | Modal de confirmação antes de excluir |
| 🔔 Notificações toast | Feedback visual de sucesso e erro |
| 📱 Responsivo | Funciona em desktop e mobile |

---

## 📁 Estrutura do Projeto

```
frontend/
├── main.py               # Aplicação Flask (backend + HTML inline)
├── requirements.txt      # Dependências Python
├── Dockerfile            # Imagem Docker multi-stage
├── docker-compose.yml    # Orquestração do container
├── .dockerignore         # Arquivos excluídos do build
├── .env                  # Variáveis de ambiente (não versionar!)
└── README.md             # Esta documentação
```

---

## 🛠 Pré-requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (para rodar via Docker)
- **ou** Python 3.10+ com `pip` (para rodar direto)

---

## ⚙️ Variáveis de Ambiente

Configure as variáveis no arquivo `.env` (nunca suba este arquivo para o Git):

| Variável | Padrão | Descrição |
|---|---|---|
| `UPLOAD_FOLDER` | `/root/dags` | Pasta onde os DAGs são salvos |
| `SECRET_KEY` | `dev-secret` | Chave criptográfica do Flask — **troque em produção!** |

**Exemplo de `.env`:**
```env
SECRET_KEY=sua-chave-secreta-longa-e-aleatoria
UPLOAD_FOLDER=/root/dags
```

> Para gerar uma `SECRET_KEY` segura:
> ```bash
> python3 -c "import secrets; print(secrets.token_hex(32))"
> ```

---

## 🚀 Rodando Localmente

### Opção 1 — Com Docker (recomendado)

Ambiente idêntico ao de produção.

```bash
# Construir a imagem e subir o container
docker compose up --build

# Rodar em background
docker compose up -d --build
```

Acesse: **http://localhost:5000**

```bash
# Parar o container
docker compose down

# Ver logs em tempo real
docker compose logs -f
```

---

### Opção 2 — Com Python direto

Útil para desenvolvimento rápido sem Docker.

```bash
# Instalar dependências
pip install flask werkzeug
```

**Linux / macOS:**
```bash
export UPLOAD_FOLDER=./dags_local
python main.py
```

**Windows (PowerShell):**
```powershell
$env:UPLOAD_FOLDER = ".\dags_local"
python main.py
```

Acesse: **http://localhost:5000**

---

## 🌐 Deploy no DigitalOcean

### 1. Copiar os arquivos para o droplet

```bash
scp -r ./frontend root@<IP_DO_DROPLET>:/root/frontend
```

### 2. Acessar o droplet via SSH

```bash
ssh root@<IP_DO_DROPLET>
cd /root/frontend
```

### 3. Criar o arquivo `.env` no servidor

```bash
echo "SECRET_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(32))')" > .env
echo "UPLOAD_FOLDER=/root/dags" >> .env
```

### 4. Subir o container

```bash
docker compose up -d --build
```

### 5. Verificar se está rodando

```bash
docker compose ps
docker compose logs -f
```

Acesse: **http://\<IP_DO_DROPLET\>:5000**

> **Dica:** Para usar um domínio com HTTPS, configure um proxy reverso como **Nginx** ou **Caddy** na frente do container.

---

## 6. Como gerar e levar para o Docker no Droplet
Quando você estiver configurando o seu droplet, em vez de versionar o .env (que agora está no seu .gitignore), você cria ele diretamente no servidor rodando os comandos abaixo:

bash
1. Entre na pasta do projeto no droplet

```bash
cd /root/frontend
```

2. Crie o arquivo .env contendo usuário, senha e a chave secreta gerada na hora pelo python

```bash
echo "APP_USERNAME=Admin" > .env
echo "APP_PASSWORD=SenhaForte" >> .env
echo "SECRET_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(32))')" >> .env
```

## 7. Parar e iniciar o container

```bash
# Parar o container
docker compose down
```

```bash
# Iniciar o container
docker compose up -d --build
```

```bash
# Ver logs em tempo real
docker compose logs -f
```

## 8. Acessando o container instalado

```bash
# Entrando no container instalado no Droplet
docker exec -it dag-manager /bin/bash
```

## 9. Acessar o frontend
Acesse: **http://<IP_DO_DROPLET>:5000**


## 🔒 Segurança

- ℹ️ **Usuário Root** — o container roda como `root` para ter permissão de acessar e salvar arquivos diretamente na pasta `/root/dags` do droplet hospedeiro.
- ✅ **Validação de extensão** — somente arquivos `.py` são aceitos.
- ✅ **Proteção contra path traversal** — verificação de `realpath` antes de excluir.
- ✅ **Tela de login integrada** — acesso restrito por usuário e senha salvos de forma segura no `.env`.
- ✅ **Build multi-stage** — a imagem final não contém ferramentas de build desnecessárias.
- ✅ **`.dockerignore`** — o arquivo `.env` nunca é copiado para dentro da imagem Docker.
- ⚠️ **SECRET_KEY** — altere para uma chave forte e aleatória em produção.


