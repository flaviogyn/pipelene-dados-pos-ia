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

![alt text](image-5.png)

3. Qual uso Metabase

![alt text](image-6.png)

3. Continuar com exemplos

![alt text](image-7.png)

4. Preferências de uso

![alt text](image-8.png)

5. Confirme

![alt text](image-4.png)

## Criando Conexação Snowflake

Acesse bando de dados

![alt text](image-9.png)

Adicionar um banco de dados 

![alt text](image-10.png)

Preencha as váriaveis de conexão

![alt text](image-11.png)

![alt text](image-12.png)

## Passo a Passo da Instalando Mapa do Brasil

Acesse as Configurações: 

* Clique no ícone de engrenagem ou de grade no canto superior direito e vá em Administração (Admin).

* Vá na aba Mapas: No menu lateral esquerdo, selecione a opção Mapas e depois clique em Adicionar um mapa.

* Preencha os Campos:
  
  * Nome: Digite um nome claro, como Brasil - Estados.URL do GeoJSON: [Insira o link direto para o arquivo de dados geográficos](https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson). 
  
  * Identificador da região (Region Identifier): 
    Defina a propriedade do JSON que contém a sigla ou código do estado (por exemplo, name, sigla ou cartodb_id dependendo do arquivo).
  
  * Nome da região (Region Name): 
    Selecione a propriedade que guarda o nome completo (por exemplo, name).
  
  * Salvar: Clique em Adicionar mapa para concluir

  ![](image-13.png)

  ![alt text](image-14.png)

  ![alt text](image-15.png)
  
## Painel Xeno Canto

```text
Dasboard
```

![alt text](image-16.png)

![alt text](image-17.png)

```text
Matriz
```

![alt text](image-18.png)



