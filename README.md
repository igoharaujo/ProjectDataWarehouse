# Projeto Data Lakehouse

## 📌 Visão Geral
Esse Projeto tem como objetivo criar um pipeline completo . A ingestão de dados ocorre a partir de um banco **OLTP**, transformando os dados em **Parquet** e armazenando-os em um **Data Lake**.

A arquitetura final seguirá o modelo **Estrela**, mas essa etapa ainda não foi implementada.

## 🏗️ Arquitetura do Projeto
### Tecnologias Utilizadas
- **Azure Data Factory (ADF)** → Ingestão e orquestração do ETL
- **Azure Data Lake** → Armazenamento dos arquivos Parquet
- **Azure Databricks** → Processamento e transformação dos dados
- **Delta Lake** → Formato otimizado para armazenamento e processamento
- **PySpark** → Manipulação e transformação dos dados

## 🔄 Processo de Ingestão
A ingestão dos dados segue os seguintes passos:

1. **Seleção das tabelas para ingestão**
   - A lista de tabelas a serem processadas está armazenada na tabela `control_ingestion` no banco OLTP.
   - Consulta SQL utilizada:
   ```sql
   SELECT  [ID], [active], [source_schema], [source_table], [query], [folder], [file_name]
   FROM [dbo].[control_ingestion]
   WHERE active = 1;
   ```
   - A coluna `active` deve ser definida como `1` para indicar que a tabela deve ser atualizada.

2. **Execução no ADF**
   - Um **Lookup** consulta a `control_ingestion`.
   - Um loop **ForEach** executa duas atividades:
     - Envia a tabela para o **Data Lake** em formato **Parquet**.
     - Chama um **notebook no Databricks** para processar os dados (**bronze ingest**).

## 🥉 Camada Bronze no Databricks

### 📌 Objetivo
A camada **Bronze** tem como função armazenar os dados brutos ingeridos no Data Lake e convertê-los para o formato **Delta Lake**.

### 🔧 Configuração Inicial
O **Databricks** realiza um **mount** para acessar os arquivos armazenados no **Data Lake**.

Comando para criação do banco de dados (executado apenas uma vez):
```sql
--CREATE DATABASE IF NOT EXISTS adventure_works_bronze
--LOCATION '/mnt/adlsstorageworksprd/landing-zone/MedallionArchitecture/Bronze/adventure_works_bronze'
```

### 🛠️ Processo de Ingestão na Camada Bronze
1. **Importação de Parâmetros**
   ```python
   %run ./Parametros
   ```
2. **Leitura dos Arquivos Parquet** e adição de **timestamp**
3. **Conversão para Delta Lake** e gravação no banco **Bronze**

Código principal:
```python
from pyspark.sql.functions import current_timestamp

def ingest_data_bronze(read_path, table_name, database_name):
    try:
        df = spark.read.parquet(read_path)
        df = df.withColumn("timestamp_bronze", current_timestamp())
        df.write.format("delta").mode("overwrite").saveAsTable(f"{database_name}.{table_name}")
        print(f"Tabela {table_name} carregada com sucesso!")
    except Exception as e:
        print(f"Erro ao carregar {table_name}: {str(e)}")
```

4. **Execução para múltiplas tabelas**
```python
for param in parameters:
    ingest_data_bronze(param["read_path"], param["table_name"], param["database_name"])
print("Processamento finalizado")
```

## 📌 Considerações Finais
- A tabela Delta é sobrescrita caso já exista (`overwrite`).
- A coluna `timestamp_bronze` ajuda a rastrear o momento da ingestão.
- A estrutura permite processamento flexível e escalável.

---

📝 **Próximos Passos:** Implementação da camada **Silver** e posteriormente **Gold**, seguindo a arquitetura Medallion.

