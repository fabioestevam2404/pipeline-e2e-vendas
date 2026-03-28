# Pipeline E2E de Vendas — Guia de Implantação

## Visão Geral

Pipeline de dados em arquitetura **Medallion (Bronze → Silver → Gold)** implementado em **PySpark + Delta Lake** sobre **Databricks** com **Unity Catalog**.

```
raw/json/ + raw/csv/
      │
      ▼
 [01_bronze.py]  → Bronze (append particionado, quarentena de corrompidos)
      │
      ▼
 [02_silver.py]  → Silver (limpeza, tipagem, deduplicação, validação, MERGE)
      │
      ▼
 [03_gold.py]    → Gold  (5 tabelas analíticas: diário, semanal, mensal, categoria, KPI)
      │
      ▼
 [04_optimize.py]→ Manutenção (OPTIMIZE, VACUUM, RBAC, monitoramento)
```

---

## Pré-requisitos

| Requisito | Detalhe |
|-----------|---------|
| Databricks Runtime | 15.4 LTS (Spark 3.5, Scala 2.12) |
| Unity Catalog | Habilitado no workspace |
| Plano | Premium ou Enterprise (Unity Catalog exige Premium+) |
| Python | 3.10+ |
| Permissões | `CREATE CATALOG`, `CREATE SCHEMA`, `CREATE TABLE`, `MODIFY`, `SELECT` |
| Storage | AWS S3, Azure ADLS Gen2 ou GCP GCS |
| Instance Profile / SP | Permissões de leitura no raw e escrita em bronze/silver/gold |

---

## Estrutura de Arquivos

```
deploy/
├── 00_config.py           # Configuração centralizada (parâmetros, paths, nomes)
├── 00_setup.py            # Setup: Unity Catalog, diretórios, validação
├── 01_bronze.py           # Ingestão JSON/CSV → Bronze Delta
├── 02_silver.py           # Limpeza + MERGE → Silver Delta
├── 03_gold.py             # Agregações → Gold Delta (5 tabelas)
├── 04_optimize.py         # Manutenção: OPTIMIZE, VACUUM, RBAC
├── pipeline_workflow.json # Definição do Databricks Workflow (JSON)
└── README.md              # Este arquivo
```

---

## Passo a Passo de Implantação

### 1. Configurar storage e credenciais

**AWS (IAM Role — recomendado):**
```bash
# Configure um Instance Profile com permissões de leitura/escrita nos buckets
# Associe o Instance Profile ao cluster no Databricks
# Não são necessários secrets para S3 com Instance Profile
```

**Azure (Service Principal):**
```bash
# Crie um App Registration no Azure AD
# Atribua a role "Storage Blob Data Contributor" no ADLS Gen2
# Configure os secrets abaixo
```

**Configurar secrets (todos os ambientes):**
```bash
# Instalar Databricks CLI
pip install databricks-cli
databricks configure --token
# Informe: host (https://<workspace>.azuredatabricks.net) e Personal Access Token

# Criar scope de secrets
databricks secrets create-scope --scope pipeline-vendas

# AWS — apenas se não usar Instance Profile
databricks secrets put --scope pipeline-vendas --key aws-access-key-id
databricks secrets put --scope pipeline-vendas --key aws-secret-access-key

# Azure
databricks secrets put --scope pipeline-vendas --key sp-client-id
databricks secrets put --scope pipeline-vendas --key sp-client-secret
databricks secrets put --scope pipeline-vendas --key sp-tenant-id

# Verificar
databricks secrets list --scope pipeline-vendas
```

### 2. Adaptar configurações em 00_config.py

Abra `00_config.py` e ajuste os paths para o seu ambiente:

```python
# Para AWS S3:
raw_path    = "s3a://meu-bucket/raw/vendas/"
bronze_path = "s3a://meu-bucket/bronze/vendas/"
silver_path = "s3a://meu-bucket/silver/vendas/"
gold_path   = "s3a://meu-bucket/gold/vendas/"

# Para Azure ADLS Gen2:
raw_path    = "abfss://raw@<storage>.dfs.core.windows.net/vendas/"
bronze_path = "abfss://bronze@<storage>.dfs.core.windows.net/vendas/"
# ... etc.

# Para Unity Catalog Volumes (recomendado — sem mounts):
raw_path    = "/Volumes/pipeline_vendas/bronze/raw/vendas/"
bronze_path = "/Volumes/pipeline_vendas/bronze/bronze/vendas/"
# ... etc.
```

### 3. Fazer upload dos arquivos para o Databricks

**Opção A — Via interface (mais simples):**
```
Databricks → Workspace → Seu diretório → Import → Upload files
```

**Opção B — Via Databricks CLI:**
```bash
# Criar diretório no workspace
databricks workspace mkdir /Repos/data-engineering/pipeline-vendas

# Upload de cada arquivo
for f in 00_config.py 00_setup.py 01_bronze.py 02_silver.py 03_gold.py 04_optimize.py; do
    databricks workspace import \
        --language PYTHON \
        --format SOURCE \
        --overwrite \
        ./$f \
        /Repos/data-engineering/pipeline-vendas/$f
done
```

**Opção C — Via Databricks Repos (recomendado para produção):**
```
Databricks → Repos → Add Repo → URL do seu repositório Git
```

### 4. Configurar os mounts (se não usar Unity Catalog Volumes)

Em `00_setup.py`, descomente e adapte a seção de mounts para o seu cloud provider:

```python
# AWS S3 com IAM Role (descomente e adapte):
mount_storage("/mnt/raw",    "s3a://meu-bucket/raw")
mount_storage("/mnt/bronze", "s3a://meu-bucket/bronze")
mount_storage("/mnt/silver", "s3a://meu-bucket/silver")
mount_storage("/mnt/gold",   "s3a://meu-bucket/gold")
```

### 5. Executar o setup inicial

```python
# No Databricks, abra o notebook 00_setup.py e execute.
# Ou via CLI:
databricks runs submit --json '{
  "existing_cluster_id": "<SEU_CLUSTER_ID>",
  "notebook_task": {
    "notebook_path": "/Repos/data-engineering/pipeline-vendas/00_setup",
    "base_parameters": {"env": "dev"}
  }
}'
```

Verifique nos logs:
- ✔ Unity Catalog criado/confirmado
- ✔ Schemas bronze, silver, gold criados
- ✔ Diretórios de storage criados
- ✔ Conectividade validada

### 6. Criar o Workflow de produção

```bash
# Via CLI (substitua os placeholders no JSON antes):
databricks jobs create --json @pipeline_workflow.json

# Verificar se foi criado:
databricks jobs list
```

**Ou via interface:**
1. Databricks → Workflows → Create Job
2. Clique em "Edit JSON" no canto superior direito
3. Cole o conteúdo de `pipeline_workflow.json`
4. Substitua os placeholders:
   - `<CAMINHO_REPO>`: caminho dos notebooks no workspace
   - `<ARN_DO_INSTANCE_PROFILE>`: ARN do IAM Role (AWS)
   - `<EMAIL_SERVICE_PRINCIPAL>`: e-mail do service principal
   - `<EMAIL_TIME_DE_DADOS>`: e-mail para alertas de falha

### 7. Executar o pipeline pela primeira vez (modo dev)

```bash
# Executar manualmente com parâmetros de dev
databricks jobs run-now \
  --job-id <JOB_ID> \
  --job-parameters '{"env": "dev", "batch_date": "2024-01-15"}'
```

Acompanhe a execução em: Databricks → Workflows → Pipeline E2E → Runs

### 8. Validar os resultados

```sql
-- No Databricks SQL ou notebook, execute:

-- Verificar tabelas criadas
SHOW TABLES IN pipeline_vendas.bronze;
SHOW TABLES IN pipeline_vendas.silver;
SHOW TABLES IN pipeline_vendas.gold;

-- Verificar contagens Bronze
SELECT COUNT(*) FROM pipeline_vendas.bronze.orders_raw;
SELECT COUNT(*) FROM pipeline_vendas.bronze.order_items_raw;

-- Verificar Quality na Silver (sem rejeitados)
SELECT COUNT(*) FROM pipeline_vendas.silver.orders     WHERE _is_valid = true;
SELECT COUNT(*) FROM pipeline_vendas.silver.order_items WHERE _is_valid = true;

-- Verificar KPIs Gold
SELECT year_month, net_revenue, total_orders, avg_order_value
FROM pipeline_vendas.gold.kpi_summary
ORDER BY year_month DESC
LIMIT 6;

-- Verificar histórico Delta (time travel funcionando)
DESCRIBE HISTORY pipeline_vendas.gold.kpi_summary;

-- Verificar linhagem (audit trail)
SELECT pipeline, source, target, record_count, status, timestamp
FROM delta.`/mnt/audit/pipeline_vendas/`
ORDER BY timestamp DESC
LIMIT 10;
```

---

## Variáveis de Parâmetro do Workflow

| Parâmetro | Descrição | Exemplo |
|-----------|-----------|---------|
| `env` | Ambiente de execução | `dev`, `staging`, `prod` |
| `batch_date` | Data do batch | `2024-01-15` ou `{{job.start_time.iso_date}}` |
| `catalog` | Nome do catálogo Unity Catalog | `pipeline_vendas` |

---

## Monitoramento e Alertas

### Verificar saúde das tabelas Delta
```sql
DESCRIBE DETAIL pipeline_vendas.gold.kpi_summary;
-- Checar: numFiles, sizeInBytes, location
```

### Verificar últimas operações
```sql
DESCRIBE HISTORY pipeline_vendas.silver.orders LIMIT 10;
```

### Time travel — consultar versão anterior
```sql
-- Por versão
SELECT * FROM pipeline_vendas.gold.kpi_summary VERSION AS OF 5;

-- Por timestamp
SELECT * FROM pipeline_vendas.gold.kpi_summary TIMESTAMP AS OF '2024-01-14 05:00:00';
```

### Restaurar para versão anterior (em caso de reprocessamento errado)
```sql
RESTORE TABLE pipeline_vendas.gold.kpi_summary TO VERSION AS OF 5;
```

### Verificar relatórios de otimização
```sql
SELECT table, files_before, files_after, space_freed_mb, batch_date
FROM delta.`/mnt/monitoring/delta_metrics/`
ORDER BY batch_date DESC
LIMIT 20;
```

---

## Estrutura de Diretórios no Storage

```
/mnt/
├── raw/vendas/
│   ├── json/           ← Arquivos JSON de pedidos (entrada)
│   └── csv/            ← Arquivos CSV de itens (entrada)
│
├── bronze/vendas/
│   ├── orders/         ← Delta table: orders_raw (particionado por _ingestion_date)
│   ├── order_items/    ← Delta table: order_items_raw
│   └── _quarantine/    ← Registros corrompidos (JSON inválido, CSV malformado)
│
├── silver/vendas/
│   ├── orders/         ← Delta table: orders (particionado por order_year/month)
│   ├── order_items/    ← Delta table: order_items (particionado por part_year/month)
│   └── _rejected/      ← Registros que falharam nas validações de negócio
│
├── gold/vendas/
│   ├── sales_daily/        ← Agregação diária (order_year, order_month)
│   ├── sales_weekly/       ← Agregação semanal (order_year)
│   ├── sales_monthly/      ← Agregação mensal (order_year)
│   ├── sales_by_category/  ← Ranking por categoria (order_year, order_month)
│   └── kpi_summary/        ← KPIs executivos (order_year)
│
├── audit/pipeline_vendas/  ← Audit trail / linhagem Delta
└── monitoring/delta_metrics/ ← Relatórios de otimização Delta
```

---

## Controle de Acesso (RBAC)

| Grupo | Bronze | Silver | Gold (todas) | Gold (KPI/mensal) |
|-------|--------|--------|--------------|-------------------|
| `data_platform` | ALL | ALL | ALL | ALL |
| `data_engineers` | SELECT + MODIFY | SELECT + MODIFY | SELECT + MODIFY | SELECT + MODIFY |
| `data_analysts` | ❌ | SELECT | SELECT | SELECT |
| `executives` | ❌ | ❌ | ❌ | SELECT |

---

## Solução de Problemas

### Job falhou na Bronze: "Path does not exist"
→ Execute `00_setup.py` manualmente para criar os diretórios e validar mounts.

### Silver falhou: "Quality check abaixo de 90%"
→ Verifique a pasta `_rejected/` para entender quais registros foram rejeitados:
```sql
SELECT _rejection_reason, COUNT(*) AS n
FROM delta.`/mnt/silver/vendas/_rejected/orders/`
GROUP BY 1 ORDER BY 2 DESC;
```

### Gold falhou: "Receita Silver × Gold diverge"
→ Reprocesse o Silver para o batch afetado com `batch_date=<data>`.

### OPTIMIZE muito lento (>30min)
→ Reduza o número de tabelas no batch de otimização ou execute em cluster maior.

### "User does not have privilege SELECT on table"
→ Execute `04_optimize.py` com um usuário que tenha `data_platform` para reconfigurar o RBAC.
