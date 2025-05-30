# 🚀 Projeto de Pipeline de Dados 

Este projeto implementa um pipeline completo de dados com foco em ingestão, transformação, carga e análise de dados financeiros. 
Dados são manipulados e analisados para geração de insights relevantes, sendo tudo orquestrado com Apache Airflow via Docker Compose.

---

##  🧰 Tecnologias e Ferramentas Utilizadas

- Python 3.10+ – Scripts ETL e API

- Apache Airflow – Orquestração de pipelines

- Docker & Docker Compose – Containerização do ambiente

- PostgreSQL – Banco de dados relacional

- FastAPI – Construção de APIs REST

- Pandas – Manipulação de dados

- SQL – Criação de tabelas e views analíticas

- SQLAlchemy / psycopg2 – Conexão com o banco de dados

- dotenv – Gerenciamento de variáveis de ambiente

- Linux / Ubuntu – Ambiente de execução

- Git – Controle de versão


## 🗂️ Estrutura do Projeto

```
.
├── api/                       # API para consulta de dados analíticos
│   └── app.py                # FastAPI com endpoints REST
│
├── projeto/                  
│   ├── dags/                 # DAGs do Airflow
│   │   └── dag_etl.py
│   ├── data/                 # Dados brutos (.csv)
│   ├── etl/                  # Scripts de ETL em Python
│   ├── logs/                 # Logs e metadados do Airflow
│   ├── plugins/              # Plugins do Airflow (se necessário)
│   ├── sql/                  # Scripts SQL de criação de tabelas e views
│   ├── .env                  # Variáveis de ambiente
│   └── docker-compose.yaml  # Arquitetura do ambiente
```

---

## 🔄 Pipeline de Dados

1. Ingestão de Dados  
   Arquivos `.csv` da pasta `data/` são lidos e preparados para ingestão.

2. Transformação e Limpeza  
   A lógica de transformação está implementada nos scripts Python dentro da pasta `etl/`.

3. Criação das Tabelas  
   As tabelas do schema `transacional` são criadas com o script SQL `create_tables.sql`.

4. Carga de Dados  
   Os dados tratados são inseridos no banco de dados RZK, no schema `transacional`.

5. Criação das Views Analíticas  
   As análises são feitas via SQL em `create_views.sql`, criando views no schema `analitico`.

6. Orquestração com Airflow  
   Todas as etapas acima são automatizadas com o Apache Airflow, que roda via Docker Compose.

---

## 🌐 API

A API (fora do Docker Compose) é construída com FastAPI e permite consultar:

- Dados da tabela `empresas`
- Views analíticas com os lucros por mês

### Exemplos de Endpoints:

GET /empresas → Lista todas as empresas
GET /empresas/{id} → Detalhes de uma empresa específica
GET /analitico/evolucao_receita
GET /analitico/gastos_salarios
GET /analitico/lucro_mes
GET /analitico/maiores_despesas
GET /analitico/melhores_clientes
GET /analitico/orcamento_consolidado
GET /analitico/transferencias_mensal

---

## 🐳 Docker Compose

O ambiente com Airflow está contido em um `docker-compose.yaml`, responsável por levantar:

- Scheduler
- Webserver
- Banco de dados
- Metadatabase do Airflow



## 🛠️ Executando o Projeto

### Subir o Airflow com Docker Compose

```bash
cd projeto/
docker-compose up --build
```

### Rodar a API

A API está fora do Docker, então rode localmente Executando o arquivo app.py:




## 👨‍💻 Autor

Projeto desenvolvido por JONATHAN NOVAIS
