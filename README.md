# Projeto Pipelene de Dados

```text
INTELIGÊNCIA ARTIFICIAL APLICADA - MODULO 02
Professores Dr: Otávio Calaça, Sirlon Diniz e Rafael Gomes
Alunos:
  Afrain da Silva Calixto
  Flávio Lourenço da Silva
  Gustavo Adolpho Souteras Barbosa
```

## Arquitetura do Projeto Cloud

```bash
https://cloud.digitalocean.com/
```

![alt text](image.png)

## Fluxo de execução

![alt text](fluxo_pipeline_xeno_canto.png)

## Origem dos dados

```bash
https://xeno-canto.org/explore/api
```

![alt text](image-9.png)

## S3

```bash
https://www.awsacademy.com/vforcesite/LMS_Login
```

Folders do S3

![alt text](xeno-canto-s3.png)

Folder Bronze/

![alt text](image-2.png)

![alt text](image-3.png)

![alt text](image-4.png)

![alt text](image-5.png)

Folder Silver/

![alt text](image-6.png)

Folder Gold/
 
![alt text](s3-gold.png)

Folder Treinamentos/

![alt text](s3-treinamento.png)

## Snowflake

```bash
https://app.snowflake.com/
```

Setup de inicial SQL

![alt text](image-10.png)

![alt text](image-11.png)

![alt text](image-12.png)

## Dbt

```bash
IP: 142.93.205.113
```

![alt text](dbt.png)


## FrontEnd (Interface Upload Dags do Airflow)

```bash
http://airflow.dcco.net.br:5000/
```

![alt text](image-14.png)

## Execuções Airflow

```bash
http://airflow.dcco.net.br:8080/
```

### ai_xeno_canto_bronze

![alt text](bronze-exec.png)

![alt text](bronze.png)

### ai_xeno_canto_silver

![alt text](silver.png)

![alt text](silver-exec.png)

### ai_xeno_canto_gold (ssh)

![alt text](image-8.png)

![alt text](image-13.png)

### ai_xeno_canto_treinamento

![alt text](treinamento.png)

![alt text](treinamento-exec.png)

## Metabase

```bash
http://142.93.249.68:3000/public/dashboard/b35e9826-8639-4a1c-995d-a06177f5f629
```


![alt text](metabase-01.png)

![alt text](metabase-02.png)

![alt text](metabase-03.png)

