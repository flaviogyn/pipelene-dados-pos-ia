# Metabase no Docker

Este diretório prepara o Metabase para rodar com Docker usando o banco interno H2 do próprio Metabase.

## Requisitos

- Docker
- Docker Compose

## Estrutura

```bash
IP: 142.93.249.68
```

```text
metabase/
├── docker-compose.yml
├── .env.example
└── README.md
```

## Copiar o script do Windows para a VPS

No PowerShell, execute a partir da raiz do projeto:

```powershell
scp docker-compose.yml root@<IP>:/root
```

Substitua `IP` pelo IP da VPS.

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
http://142.93.249.68:3000
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

## Observações

O compose usa apenas o volume persistente do Metabase para o banco H2:

```text
metabase_data:/metabase-data
```

## Variáveis de ambiente

O arquivo `.env` pode conter apenas:

```env
MB_PORT=3000
```

## Setup do Metabase

1. Passo acesse a URL e clique em Let's get started

```bash
http://142.93.249.68:3000/setup
```

![alt text](image.png)

2. Crie usuário e senha

![alt text](image-1.png)

3. Escolher idioma e continuar com exemplos

![alt text](image-2.png)

4. Preferências de uso

![alt text](image-3.png)

5. Confirme

![alt text](image-4.png)

