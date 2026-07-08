# Instalação e Implantação do PostgreSQL no Docker (DigitalOcean)

Este guia orienta o processo de configuração e implantação do PostgreSQL em um contêiner Docker rodando em uma máquina virtual (Droplet) da DigitalOcean.

---

## 1. Pré-requisitos

*   Uma conta ativa na [DigitalOcean](https://www.digitalocean.com/).
*   Acesso SSH configurado ou chave SSH adicionada na sua conta DigitalOcean.
*   Conhecimento básico do terminal Linux.

---

## 2. Passo 1: Criar o Droplet na DigitalOcean

1.  Acesse o painel da DigitalOcean e clique em **Create** > **Droplets**.
2.  **Choose Region**: Escolha a região mais próxima dos seus usuários ou da sua infraestrutura (ex: *NYC*, *SFO* ou *AMS*).
3.  **Choose an image**: Selecione **Ubuntu** (versão LTS mais recente, ex: `24.04 LTS` ou `22.04 LTS`).
4.  **Choose Size**: Escolha o plano básico. Para fins de teste/estudo, o plano básico com **1 GB RAM / 1 CPU / 25 GB SSD** (Shared CPU) é suficiente.
5.  **Authentication**: Selecione **SSH Key** (recomendado para maior segurança) e selecione a sua chave.
6.  Clique em **Create Droplet**. Aguarde a criação e copie o IP público fornecido.

---

## 3. Passo 2: Configurar o Cloud Firewall da DigitalOcean

Por padrão, expor a porta do banco de dados (`5432`) diretamente para toda a internet é um grande risco de segurança. **Siga estes passos para proteger seu banco de dados:**

1.  No menu lateral da DigitalOcean, vá em **Networking** > **Firewalls**.
2.  Clique em **Create Firewall**.
3.  Dê um nome para o firewall (ex: `postgres-firewall`).
4.  **Inbound Rules** (Regras de Entrada):
    *   **SSH (Porta 22)**: Permita de `All IPv4` e `All IPv6` (ou apenas de seus IPs específicos).
    *   **PostgreSQL (Porta 5432)**: Adicione uma regra do tipo *Custom* com a porta `5432`. No campo **Sources**, adicione **apenas** os IPs que precisam acessar o banco (como o IP da máquina do Airflow, o IP da sua máquina local ou do dbt). **Nunca** deixe `All IPv4` / `All IPv6`.
5.  **Apply to Droplets**: Busque pelo nome do Droplet criado no Passo 1.
6.  Clique em **Create Firewall**.

---

## 4. Passo 3: Acessar o Droplet e Instalar o Docker / Docker Compose

1.  Abra o seu terminal local e conecte-se ao Droplet via SSH:
    ```bash
    ssh root@<IP_DO_SEU_DROPLET>
    ```

2.  Atualize o sistema:
    ```bash
    sudo apt update && sudo apt upgrade -y
    ```

3.  Instale o Docker e Docker Compose:
    ```bash
    sudo apt install docker.exe docker-compose-v2 -y
    ```
    *(Nota: Em algumas distribuições do Ubuntu, pode ser necessário rodar `sudo apt install docker-compose` ou seguir a documentação oficial do Docker)*

4.  Verifique se a instalação foi bem-sucedida:
    ```bash
    docker --version
    docker compose version
    ```

---

## 5. Passo 4: Clonar e Rodar o PostgreSQL

1.  No Droplet, crie ou navegue até o diretório onde deseja instalar:
    ```bash
    mkdir -p /app/pipeline-dados
    cd /app/pipeline-dados
    ```

2.  Crie os arquivos necessários no servidor. Você pode clonar o repositório Git ou criá-los manualmente usando `nano`:

    *   **Criar arquivo `.env`**:
        ```bash
        nano .env
        ```
        Copie o conteúdo abaixo, altere para senhas fortes e salve (Ctrl+O, Enter, Ctrl+X):
        ```env
        POSTGRES_USER=postgres
        POSTGRES_PASSWORD=sua_senha_altamente_segura_aqui
        POSTGRES_DB=db_dados
        POSTGRES_PORT=5432
        ```

    *   **Criar arquivo `docker-compose.yml`**:
        ```bash
        nano docker-compose.yml
        ```
        Cole o conteúdo a seguir e salve:
        ```yaml
        version: '3.8'

        services:
          postgres:
            image: postgres:16-alpine
            container_name: postgres_db
            restart: always
            environment:
              POSTGRES_USER: ${POSTGRES_USER}
              POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
              POSTGRES_DB: ${POSTGRES_DB}
            ports:
              - "${POSTGRES_PORT}:5432"
            volumes:
              - postgres_data:/var/lib/postgresql/data
            healthcheck:
              test: ["CMD-SHELL", "pg_isready -U $$POSTGRES_USER -d $$POSTGRES_DB"]
              interval: 10s
              timeout: 5s
              retries: 5

        volumes:
          postgres_data:
            driver: local
        ```

3.  Suba o contêiner em segundo plano (modo detached):
    ```bash
    docker compose up -d
    ```

4.  Verifique se o contêiner está rodando e saudável:
    ```bash
    docker compose ps
    ```

---

## 6. Comandos Úteis do Docker

*   **Ver logs do banco de dados**:
    ```bash
    docker compose logs -f postgres
    ```
*   **Parar o contêiner (sem perder os dados)**:
    ```bash
    docker compose down
    ```
*   **Iniciar o contêiner novamente**:
    ```bash
    docker compose up -d
    ```
*   **Entrar no terminal do PostgreSQL (psql) diretamente no contêiner**:
    ```bash
    docker exec -it postgres_db psql -U postgres -d db_dados
    ```

---

## 7. Como se Conectar ao Banco de Dados

*   **Host**: `<IP_DO_SEU_DROPLET>`
*   **Porta**: `5432`
*   **User**: `postgres` (ou o valor definido no `.env`)
*   **Password**: O valor definido no `.env`
*   **Database**: `db_dados` (ou o valor definido no `.env`)

*Dica: Você pode usar ferramentas visuais como **DBeaver** ou **pgAdmin** em sua máquina local para se conectar a este banco de dados, contanto que o IP da sua máquina esteja liberado nas Inbound Rules do Firewall da DigitalOcean.*
