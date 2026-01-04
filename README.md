# Projeto – Pipeline Receita Federal - [WIP - Work in Progress]

**PT-BR:** Este projeto implementa um **pipeline de dados** utilizando **Apache Airflow** para orquestrar a coleta, processamento e armazenamento dos **dados públicos de CNPJ disponibilizados pela Receita Federal do Brasil**.

A arquitetura segue uma abordagem **ETL/ELT em camadas (Raw → Bronze → Silver → Gold)**, permitindo organização, rastreabilidade e fácil evolução do pipeline.

---

**EN**: This project implements a **data pipeline** using **Apache Airflow** to orchestrate the collection, processing, and storage of public CNPJ **data provided by the Brazilian Federal Revenue Service.**

The architecture follows a layered **ETL/ELT approach (Raw → Bronze → Silver → Gold)**, enabling organized, traceable, and scalable pipelines.

---

## Objetivos do Projeto / Project Objectives 

**PT-BR**:
* Automatizar a ingestão mensal de dados de CNPJ
* Garantir reprodutibilidade e rastreabilidade
* Fornecer base sólida para análises, estudos e aplicações
* Gerar relatórios e análises automáticas

**EN**:
* Automate the monthly ingestion of CNPJ data
* Ensure reproducibility and traceability
* Provide a solid foundation for analysis, research, and applications
* Enable automated reports and analytics

---

## Estrutura Geral do Projeto 

```
cnpj-data-pipeline/
├── docker-compose.yml
├── Dockerfile
├── .env
├── requirements.txt
├── README.md
├── warehouse/
├── data/
│   ├── raw/
│   ├── bronze/
│   │   ├── csv/
│   │   └── parquet/
│   ├── silver/
│   └── gold/
│
└── airflow/
    ├── dags/
    │   └── pipeline_dag.py
    ├── logs/
    └── plugins/
        └── cnpj_pipeline
            ├── extract/
            │   ├── decrompress.py      
            │   └── downloader.py
            ├── load/
            └── transform/
```
##  Arquivos de Infraestrutura / Infrastructure Files

### `docker-compose.yml`

**PT-BR: Responsável por orquestrar o ambiente local:**  

* Sobe o container do **Apache Airflow**
* Gerencia volumes persistentes (`data`, `warehouse`, `airflow`)
* Injeta variáveis de ambiente via `.env`

**EN: Orchestrates the local environment:** 

* Launches the Apache Airflow container
* Manages persistent volumes (data, warehouse, airflow)
* Injects environment variables from .env

---

### `Dockerfile`

**PT-BR: Define a imagem Docker do projeto:**
* Baseada em `python:3.11-slim`
* Instala dependências do projeto e bibliotecas necessárias
* Configura variáveis de ambiente do Airflow

**EN: Defines the Docker image:**
* Based on python:3.11-slim
* Installs project dependencies and necessary libraries
* Configures Airflow environment variables

---

### `.env`

**PT-BR: Centraliza variáveis de configuração:**
* URLs da Receita Federal
* Caminhos de dados (`raw`, `bronze`, etc.)
* Configurações do Airflow
* Credenciais de acesso ao Airflow

**EN: Centralizes configuration variables:**
* Federal Revenue URLs
* Data paths (raw, bronze, etc.)
* Airflow settings
* Access credentials

---

### `requirements.txt`

**PT-BR: Lista de dependências Python do projeto, incluindo:**
* Apache Airflow
* Requests
* BeautifulSoup
* Pendulum
* Bibliotecas auxiliares de processamento

**EN: Lists Python dependencies, including:**
* Airflow, Requests 
* BeautifulSoup 
* Pendulum
* Auxiliary libraries.

---

## Camadas de Dados / Data Layers (data/)

A pasta `data/` segue o padrão de **arquitetura em camadas**

### `raw/`

**PT**: Armazena arquivos ZIP brutos da Receita Federal, organizados por período (YYYY-MM). Nenhum tratamento aplicado.

**EN:** Stores raw ZIP files from Receita Federal, organized by period (YYYY-MM). No transformations applied.

---

### `bronze/`

**PT:** Contém arquivos descompactados, seja em csv ou partquet, mantendo o formato original, base para transformações futuras.
**EN:** Contains decompressed files, maintaining the original format, serving as the base for further transformations.

---

### `silver/` (TODO)  

**PT:** Dados limpos, normalizados e estruturados, ideal para validação e modelagem relacional.

**EN:** Cleaned, normalized, and structured data; ideal for validation and relational modeling.

---

### `gold/` (TODO) 

**PT:** Camada final para consumo analítico, agregações, métricas e tabelas de negócio.

**EN:** Final layer for analytical consumption, including aggregations, KPIs, and business tables.

---

### `warehouse/` (TODO) 

**PT:** Armazenamento analítico local (ex.: DuckDB), otimizado para consultas e análises.

**EN:** Local analytical storage (e.g., DuckDB), optimized for queries and analysis.

---

### Airflow (`airflow/`)

#### `dags/`

**PT: Define o fluxo principal em csv:**
* Download dos dados da Receita Federal
* Descompactação dos arquivos
* Organização por período

**EN: Defines the main pipeline flow:**
* Download data from the Receita Federal
* Decompress files
* Organize by period
---
**PT**: Contém as DAGs do Airflow. Ex.: pipeline_dag.py define o fluxo principal de download, descompressão e organização por período.

**EN****: Contains Airflow DAGs. Ex.: pipeline_dag.py defines the main pipeline flow: download, decompression, and organization by period.

##### `pipeline_csv_dag.py`

**PT: Define o fluxo principal em CSV:**
* Download dos dados da Receita Federal
* Descompactação dos arquivos
* Organização por período

**EN: Defines the main pipeline flow in CSV:**
* Download data from Receita Federal
* Decompress files
* Organize by period

##### `pipeline_parquet_dag.py`

**PT: Define o fluxo principal em Parquet:**
* Download dos dados da Receita Federal
* Descompactação dos arquivos
* Conversão para Parquet
* Organização por período

**EN: Defines the main pipeline flow in Parquet:**

* Download data from Receita Federal
* Decompress files
* Convert to Parquet
* Organize by period

---

### `logs/`
**PT: Logs de execução das DAGs e tasks do Airflow.**\
**EN: Execution logs of Airflow DAGs and tasks.**

### `plugins/`
**PT: Código-fonte do pipeline, organizado por responsabilidade.**\
**EN: Pipeline source code, organized by responsibility.**

### `extract/`

#### **PT: Responsável pela extração dos dados:** 
* downloader.py: faz o download dos arquivos ZIP por período
* decompress.py: realiza a descompactação dos arquivos

#### **EN: Responsible for data extraction:**
* downloader.py: downloads ZIP files per period
* decompress.py: decompresses the files

### `transform/`

#### **PT: Camada de transformação de dados:**
* Integrações com dbt
* Regras de negócio e modelagem

#### **EN: Data transformation layer:**
* Integrations with dbt
* Business rules and modeling

### `load/`

**PT: Camada responsável por carregar dados processados no warehouse.**\
**EN: Layer responsible for loading processed data into the warehouse.**
Claro! Aqui está a seção formatada corretamente em **Markdown**, mantendo o paralelo **Português ↔ Inglês** e destacando comandos com blocos de código:


## Acesso ao Airflow (Modo Desenvolvimento) / Airflow Access (Development Mode)

**PT:** Este projeto utiliza o modo `airflow standalone` para facilitar o desenvolvimento local. Ao subir o ambiente com Docker, o Airflow cria automaticamente um usuário administrador (`admin`) com uma **senha gerada dinamicamente**, exibida nos logs do container.  
**EN:** This project uses `airflow standalone` to simplify local development. When starting the Docker environment, Airflow automatically creates an admin user (`admin`) with a **dynamically generated password**, displayed in the container logs.

### Obter a senha gerada automaticamente / Retrieve the auto-generated password

```bash
docker logs cnpj-data-pipeline

# Exemplo de saída:
# username: admin
# password: <generated-password>
````

### (Optional) Redefinir a senhclear
a do usuário admin / Reset admin user password

**PT:** Para facilitar o acesso em ambiente de desenvolvimento, é possível redefinir manualmente a senha do usuário administrador executando:
**EN:** For easier access in a development environment, you can manually reset the admin user password by running:

```bash
docker exec -it cnpj-data-pipeline bash

airflow users reset-password --username admin --password admin
```

**PT:** Esta etapa é opcional e recomendada apenas para ambientes de desenvolvimento. Em produção, o gerenciamento de usuários e credenciais deve ser feito de forma segura.                                              
**EN:** This step is optional and recommended only for development environments. In production, user and credential management should follow secure practices.