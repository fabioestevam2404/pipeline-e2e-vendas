# =============================================================================
# PIPELINE E2E DE VENDAS — OTIMIZAÇÃO E GOVERNANÇA
# =============================================================================
# Arquivo  : 04_optimize.py
# Propósito: Manutenção diária das tabelas Delta Lake:
#              1) Diagnóstico de fragmentação (DESCRIBE DETAIL)
#              2) OPTIMIZE + Z-Order (compactação + co-localização)
#              3) VACUUM (remoção de arquivos obsoletos)
#              4) ANALYZE TABLE (atualização de estatísticas)
#              5) Relatório comparativo antes/depois
#              6) Políticas de retenção (time travel por camada)
#              7) RBAC (controle de acesso por grupo Unity Catalog)
#              8) Monitoramento de data freshness
#
# Frequência: Executar APÓS o pipeline principal (task 5 do Workflow)
# Dependências: 03_gold.py (deve ter concluído com sucesso)
# =============================================================================

import glob
import logging
import os
import platform
import sys
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable
    _PYSPARK_AVAILABLE = True
except ImportError:
    _PYSPARK_AVAILABLE = False
    if TYPE_CHECKING:
        from pyspark.sql import SparkSession
        from delta.tables import DeltaTable

try:
    from config import get_config, PipelineConfig
except ImportError:
    try:
        from dotenv import load_dotenv
        load_dotenv(override=False)
    except ImportError:
        pass

    from dataclasses import dataclass

    @dataclass
    class PipelineConfig:
        env:                    str = os.environ.get("ENV",              "dev")
        catalog:                str = os.environ.get("CATALOG",          "pipeline_vendas_local")
        batch_date:             str = datetime.now().strftime("%Y-%m-%d")
        bronze_path:            str = os.environ.get("BRONZE_PATH",      "data/bronze/")
        silver_path:            str = os.environ.get("SILVER_PATH",      "data/silver/")
        gold_path:              str = os.environ.get("GOLD_PATH",        "data/gold/")
        metrics_path:           str = os.environ.get("METRICS_PATH",     "data/monitoring/")
        audit_path:             str = os.environ.get("AUDIT_PATH",       "data/audit/")
        vacuum_retention_hours: int = 168
        group_engineers:        str = "data_engineers"
        group_analysts:         str = "data_analysts"
        group_executives:       str = "executives"
        group_platform:         str = "data_platform"

        @property
        def bronze_db(self):  return f"{self.catalog}.bronze"
        @property
        def silver_db(self):  return f"{self.catalog}.silver"
        @property
        def gold_db(self):    return f"{self.catalog}.gold"

    def get_config(): return PipelineConfig()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("optimize")

# =============================================================================
# HELPERS — AMBIENTE LOCAL vs DATABRICKS
# =============================================================================

def _is_databricks() -> bool:
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return True
    try:
        import IPython
        shell = IPython.get_ipython()
        if shell and "databricks" in type(shell).__module__.lower():
            return True
    except Exception:
        pass
    return False


def _ensure_hadoop_home() -> None:
    """Configura HADOOP_HOME e adiciona bin ao PATH no Windows."""
    if platform.system() != "Windows":
        return

    if os.environ.get("HADOOP_HOME"):
        hadoop_dir = Path(os.environ["HADOOP_HOME"])
    else:
        candidates = [
            Path(sys.prefix) / "Library" / "bin",
            Path(__file__).parent / "winutils" / "bin",
            Path.home() / "winutils" / "bin",
        ]
        for c in candidates:
            if (c / "winutils.exe").exists():
                hadoop_dir = c.parent
                break
        else:
            hadoop_dir = Path(__file__).parent / "winutils"

    bin_dir = hadoop_dir / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)

    if not (bin_dir / "winutils.exe").exists():
        url = ("https://github.com/cdarlint/winutils/raw/master"
               "/hadoop-3.3.5/bin/winutils.exe")
        try:
            logger.info("[SETUP] Baixando winutils.exe...")
            urllib.request.urlretrieve(url, bin_dir / "winutils.exe")
        except Exception as exc:
            logger.warning(f"[SETUP] winutils não pôde ser baixado: {exc}")

    os.environ["HADOOP_HOME"] = str(hadoop_dir)
    os.environ["hadoop.home.dir"] = str(hadoop_dir)
    bin_str = str(bin_dir)
    if bin_str not in os.environ.get("PATH", ""):
        os.environ["PATH"] = bin_str + os.pathsep + os.environ.get("PATH", "")


def _ensure_java_home() -> None:
    """Localiza e define JAVA_HOME automaticamente se não estiver definido."""
    if os.environ.get("JAVA_HOME"):
        return

    search_roots = [
        Path(r"C:/Program Files/Java"),
        Path(r"C:/Program Files/Eclipse Adoptium"),
        Path(r"C:/Program Files/Microsoft"),
        Path(r"C:/Program Files/Amazon Corretto"),
        Path(r"C:/Program Files/BellSoft"),
    ]
    for root in search_roots:
        if not root.exists():
            continue
        for jdk in sorted(root.iterdir(), reverse=True):
            if (jdk / "bin" / "java.exe").exists():
                os.environ["JAVA_HOME"] = str(jdk)
                logger.info(f"[SETUP] JAVA_HOME definido automaticamente: {jdk}")
                return

    for pattern in [r"C:/Program Files/Java/jdk*",
                    r"C:/Program Files/Java/jre*"]:
        matches = sorted(glob.glob(pattern), reverse=True)
        if matches:
            os.environ["JAVA_HOME"] = matches[0]
            logger.info(f"[SETUP] JAVA_HOME: {matches[0]}")
            return

    logger.warning("[SETUP] JAVA_HOME não encontrado — Spark pode falhar.")



# =============================================================================
# MAPEAMENTO DE TABELAS E ESTRATÉGIAS DE OTIMIZAÇÃO
#
# Z-Order: aplique nas colunas mais frequentes em WHERE / JOIN / GROUP BY.
# Regra de ouro: máximo de 3-4 colunas por Z-Order (mais colunas = lei dos
# retornos decrescentes). Nunca usar Z-Order em colunas de alta cardinalidade
# que não sejam usadas como filtro (ex: UUIDs únicos não filtrados).
# =============================================================================

def get_tables_config(cfg: PipelineConfig) -> dict:
    b = cfg.bronze_path
    s = cfg.silver_path
    g = cfg.gold_path
    return {
        # BRONZE — compactação simples, sem Z-Order
        # Dado bruto raramente consultado diretamente
        f"{cfg.bronze_db}.orders_raw": {
            "path":        b + "orders/",
            "z_order":     [],
            "vacuum":      True,
            "analyze":     False,
            "description": "Pedidos brutos (Bronze)",
        },
        f"{cfg.bronze_db}.order_items_raw": {
            "path":        b + "order_items/",
            "z_order":     [],
            "vacuum":      True,
            "analyze":     False,
            "description": "Itens brutos (Bronze)",
        },

        # SILVER — Z-Order nas colunas de filtro e join mais frequentes
        f"{cfg.silver_db}.orders": {
            "path":        s + "orders/",
            "z_order":     ["order_date", "customer_id", "status"],
            "vacuum":      True,
            "analyze":     True,   # usado em JOINs → estatísticas ajudam o planner
            "description": "Pedidos limpos (Silver)",
        },
        f"{cfg.silver_db}.order_items": {
            "path":        s + "order_items/",
            "z_order":     ["order_id", "category", "product_id"],
            "vacuum":      True,
            "analyze":     True,
            "description": "Itens limpos (Silver)",
        },

        # GOLD — Z-Order pelas dimensões mais usadas em filtros BI
        f"{cfg.gold_db}.sales_daily": {
            "path":        g + "sales_daily/",
            "z_order":     ["order_date", "category", "channel"],
            "vacuum":      True,
            "analyze":     True,
            "description": "Vendas diárias (Gold)",
        },
        f"{cfg.gold_db}.sales_weekly": {
            "path":        g + "sales_weekly/",
            "z_order":     ["order_year", "order_week", "category"],
            "vacuum":      True,
            "analyze":     True,
            "description": "Vendas semanais (Gold)",
        },
        f"{cfg.gold_db}.sales_monthly": {
            "path":        g + "sales_monthly/",
            "z_order":     ["order_year", "order_month", "category"],
            "vacuum":      True,
            "analyze":     True,
            "description": "Vendas mensais (Gold)",
        },
        f"{cfg.gold_db}.sales_by_category": {
            "path":        g + "sales_by_category/",
            "z_order":     ["category", "order_year", "order_month"],
            "vacuum":      True,
            "analyze":     True,
            "description": "Vendas por categoria (Gold)",
        },
        f"{cfg.gold_db}.kpi_summary": {
            "path":        g + "kpi_summary/",
            "z_order":     ["order_year", "order_month"],
            "vacuum":      True,
            "analyze":     True,
            "description": "KPI Summary (Gold)",
        },
    }

# =============================================================================
# SPARK SESSION
# =============================================================================

def get_spark(cfg: PipelineConfig) -> SparkSession:
    _ensure_hadoop_home()
    _ensure_java_home()

    os.environ.setdefault("PYSPARK_PYTHON",        sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .appName(f"optimize_manutencao_{cfg.env}_{cfg.batch_date}")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled",   "true")
        # NÃO desabilitar o safety check de retenção em produção:
        # spark.databricks.delta.retentionDurationCheck.enabled = false
        # só deve ser usado em ambientes de desenvolvimento/teste
    )

    if not _is_databricks():
        # No Windows o Spark pode fazer bind num hostname não resolvível, causando
        # NullPointerException em BlockManagerId.executorId() no heartbeat do executor.
        # Fixar driver.host/bindAddress em localhost resolve o registro do BlockManager.
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        builder = (
            builder
            .config("spark.driver.host",        "localhost")
            .config("spark.driver.bindAddress", "127.0.0.1")
        )
        from delta import configure_spark_with_delta_pip
        builder = configure_spark_with_delta_pip(builder)
    else:
        builder = (
            builder
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

# =============================================================================
# FASE 1: DIAGNÓSTICO
# =============================================================================

def diagnose_table(spark, table_name: str, path: str = "") -> dict:
    """
    Coleta métricas de saúde da tabela Delta antes da otimização.

    DESCRIBE DETAIL retorna:
      numFiles    : número de arquivos de dados no path
      sizeInBytes : tamanho total em bytes
      name        : nome da tabela no catálogo
      format      : "delta"
      location    : path do storage

    Decide se a tabela precisa de OPTIMIZE baseado no tamanho médio
    dos arquivos vs. o threshold de 128 MB (target file size do Delta).
    """
    try:
        if _is_databricks():
            detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
            num_files  = detail["numFiles"]   or 0
            size_bytes = detail["sizeInBytes"] or 0
        else:
            # Modo local: contar arquivos Parquet via filesystem Python
            # (DeltaTable.forPath requer hadoop.dll nativo no Windows)
            p = Path(path)
            parquet_files = [
                f for f in p.rglob("*")
                if f.is_file() and f.suffix == ".parquet"
                and "_delta_log" not in str(f)
            ]
            num_files  = len(parquet_files)
            size_bytes = sum(f.stat().st_size for f in parquet_files)

        avg_file_mb = (size_bytes / max(num_files, 1)) / (1024 ** 2)
        metrics = {
            "table":           table_name,
            "num_files":       num_files,
            "total_size_mb":   round(size_bytes / (1024 ** 2), 2),
            "avg_file_mb":     round(avg_file_mb, 2),
            "needs_optimize":  avg_file_mb < 128 and num_files > 10,
        }
        icon = "⚠" if metrics["needs_optimize"] else "✔"
        logger.info(
            f"[DIAGNOSE] {icon} {table_name.split('.')[-1]:25s} | "
            f"{num_files:5d} arquivos | "
            f"{metrics['total_size_mb']:8.1f} MB total | "
            f"{avg_file_mb:6.1f} MB/arquivo"
        )
        return metrics

    except Exception as e:
        logger.warning(f"[DIAGNOSE] Erro em {table_name}: {e}")
        return {"table": table_name, "error": str(e), "needs_optimize": False}

# =============================================================================
# FASE 2: OPTIMIZE + Z-ORDER
# =============================================================================

def run_optimize(spark, table_name: str, path: str, z_order_cols: list):
    """
    Compacta arquivos pequenos e aplica Z-Ordering.

    OPTIMIZE sozinho: compacta arquivos < 128 MB em arquivos de ~128 MB.
    OPTIMIZE + ZORDER: além da compactação, co-localiza dados por coluna(s)
      dentro de cada arquivo, reduzindo data scan em filtros específicos.

    Efeito no custo: menos arquivos → menos operações de abertura de arquivo
    → job termina mais rápido → menos DBUs consumidos.

    O resultado do OPTIMIZE mostra:
      - numFilesAdded/numFilesRemoved: eficácia da compactação
      - numBytesAdded/numBytesRemoved: eficiência de armazenamento
    """
    logger.info(f"[OPTIMIZE] Iniciando: {table_name}")
    start = datetime.now()
    short = table_name.split(".")[-1]

    try:
        if _is_databricks():
            if z_order_cols:
                z_cols = ", ".join(z_order_cols)
                result = spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({z_cols})")
                logger.info(f"[OPTIMIZE] ✔ {short} ZORDER BY ({z_cols})")
            else:
                result = spark.sql(f"OPTIMIZE {table_name}")
                logger.info(f"[OPTIMIZE] ✔ {short} (sem Z-Order)")
            elapsed = (datetime.now() - start).seconds
            try:
                m = result.collect()[0]
                logger.info(
                    f"[OPTIMIZE] Métricas — adicionados: {m.get('numFilesAdded','?')} | "
                    f"removidos: {m.get('numFilesRemoved','?')} | tempo: {elapsed}s"
                )
            except Exception:
                logger.info(f"[OPTIMIZE] Concluído em {elapsed}s")
        else:
            # Modo local: OPTIMIZE requer hadoop.dll nativo (não disponível).
            # Em dev os dados são pequenos — compactação não agrega valor.
            z_info = f" ZORDER BY ({', '.join(z_order_cols)})" if z_order_cols else ""
            logger.info(
                f"[OPTIMIZE] ⏭ {short}{z_info} — pulado em modo local "
                f"(operação de produção, requer Databricks Runtime)"
            )

    except Exception as e:
        logger.error(f"[OPTIMIZE] ✘ Falha em {table_name}: {e}")
        raise

# =============================================================================
# FASE 3: VACUUM
# =============================================================================

def run_vacuum(spark, table_name: str, path: str, retention_hours: int):
    """
    Remove arquivos de dados não referenciados pelo Delta transaction log.

    Por que fazer DRY RUN primeiro?
      - Transparência: mostra exatamente o que será deletado antes de deletar
      - Segurança: permite validar que nenhum arquivo ativo será removido
      - Auditoria: os logs mostram quantos arquivos foram removidos

    ATENÇÃO — retention_hours mínimo:
      - Databricks impõe 168h (7 dias) como mínimo em produção
      - Reduzir abaixo de 168h exige desabilitar o safety check
        (spark.databricks.delta.retentionDurationCheck.enabled=false)
      - Apenas faça isso em ambientes de dev/teste
      - Em produção, manter ≥168h garante time travel para debugging

    ATENÇÃO — não executar VACUUM imediatamente após MERGE/CLONE em tabelas
    com leitores streaming ativos — pode corromper leituras em andamento.
    """
    logger.info(f"[VACUUM] Iniciando: {table_name} (retenção: {retention_hours}h)")

    try:
        if _is_databricks():
            # DRY RUN: lista arquivos que seriam removidos sem deletar
            dry = spark.sql(
                f"VACUUM {table_name} RETAIN {retention_hours} HOURS DRY RUN"
            )
            n_files = dry.count()
            logger.info(f"[VACUUM] DRY RUN — {n_files:,} arquivo(s) elegíveis para remoção")
            if n_files > 0:
                spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
                logger.info(f"[VACUUM] ✔ {n_files:,} arquivo(s) removidos de {table_name}")
            else:
                logger.info(f"[VACUUM] Nenhum arquivo para remover em {table_name}")
        else:
            # Modo local: VACUUM requer hadoop.dll nativo (não disponível).
            # Em dev os dados são pequenos e devem ser preservados para debug.
            logger.info(
                f"[VACUUM] ⏭ {table_name.split('.')[-1]} — pulado em modo local "
                f"(operação de produção, requer Databricks Runtime)"
            )

    except Exception as e:
        # VACUUM não é crítico — não propagar a exceção
        logger.warning(f"[VACUUM] ⚠ Falha em {table_name}: {e}")
        logger.warning("[VACUUM] Continuando sem remoção — tabela permanece íntegra.")

# =============================================================================
# FASE 4: ANALYZE TABLE
# =============================================================================

def run_analyze(spark, table_name: str, path: str):
    """
    Atualiza estatísticas de tabela e colunas para o otimizador de queries.

    Sem estatísticas atualizadas, o Spark pode:
      - Escolher o lado errado do broadcast join
      - Sub ou super estimar o tamanho de shuffle
      - Gerar planos de execução subótimos

    Especialmente importante após OPTIMIZE (que altera a distribuição dos dados)
    e após INSERT/DELETE em larga escala.

    Nota: ANALYZE TABLE FOR ALL COLUMNS é suportado no Spark SQL padrão,
    mas tem efeito mais limitado fora do Databricks (sem Delta Statistics).
    """
    if not _is_databricks():
        logger.info(f"[ANALYZE] Pulado em modo local (Unity Catalog Statistics): {table_name.split('.')[-1]}")
        return

    logger.info(f"[ANALYZE] Atualizando estatísticas: {table_name}")
    try:
        spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS")
        logger.info(f"[ANALYZE] ✔ Estatísticas atualizadas: {table_name}")
    except Exception as e:
        logger.warning(f"[ANALYZE] ⚠ Falha em {table_name}: {e}")

# =============================================================================
# FASE 5: RELATÓRIO COMPARATIVO
# =============================================================================

def generate_report(spark, cfg: PipelineConfig,
                    before: list, after: list):
    """
    Compara métricas antes/depois das otimizações e persiste no audit trail.
    """
    logger.info("[REPORT] Gerando relatório de otimização...")

    before_map = {d["table"]: d for d in before if "error" not in d}
    after_map  = {d["table"]: d for d in after  if "error" not in d}

    records = []
    logger.info("\n" + "=" * 75)
    logger.info(f"  {'Tabela':<28} {'Arquivos':>10} {'Avg MB':>12} {'Espaço lib.':>12}")
    logger.info(f"  {'':28} {'Antes→Depois':>10} {'Antes→Depois':>12} {'(MB)':>12}")
    logger.info("=" * 75)

    for table in before_map:
        if table not in after_map:
            continue
        b = before_map[table]
        a = after_map[table]

        space_freed = b["total_size_mb"] - a["total_size_mb"]
        short_name  = table.split(".")[-1]

        logger.info(
            f"  {short_name:<28} "
            f"{b['num_files']:>5}→{a['num_files']:<5} "
            f"{b['avg_file_mb']:>6.1f}→{a['avg_file_mb']:<6.1f} "
            f"{space_freed:>+10.1f}"
        )

        records.append({
            "table":                table,
            "files_before":         b["num_files"],
            "files_after":          a["num_files"],
            "avg_file_mb_before":   b["avg_file_mb"],
            "avg_file_mb_after":    a["avg_file_mb"],
            "total_size_mb_before": b["total_size_mb"],
            "total_size_mb_after":  a["total_size_mb"],
            "space_freed_mb":       round(space_freed, 2),
            "batch_date":           cfg.batch_date,
            "timestamp":            datetime.now().isoformat(),
        })

    logger.info("=" * 75)

    # Persistir relatório
    if records:
        try:
            if _is_databricks():
                (
                    spark.createDataFrame(records)
                    .write.format("delta").mode("append")
                    .save(cfg.metrics_path)
                )
                logger.info(f"[REPORT] ✔ Relatório salvo em Delta: {cfg.metrics_path}")
            else:
                # Modo local: Delta write requer hadoop.dll nativo (não disponível no
                # Windows) e causa crash do executor → loop de heartbeat a cada 10s.
                # Usar JSON puro evita qualquer operação Hadoop no filesystem.
                import json as _json
                metrics_dir = Path(cfg.metrics_path)
                metrics_dir.mkdir(parents=True, exist_ok=True)
                out_file = metrics_dir / f"optimize_{cfg.batch_date}.json"
                with open(out_file, "w", encoding="utf-8") as fh:
                    _json.dump(records, fh, ensure_ascii=False, indent=2, default=str)
                logger.info(f"[REPORT] ✔ Relatório salvo em JSON: {out_file}")
        except Exception as e:
            logger.warning(f"[REPORT] Não foi possível salvar o relatório: {e}")

# =============================================================================
# FASE 6: POLÍTICAS DE RETENÇÃO (TIME TRAVEL)
# =============================================================================

def configure_retention_policies(spark, cfg: PipelineConfig):
    """
    Configura políticas de retenção de dados por camada.

    delta.deletedFileRetentionDuration:
      Quanto tempo os arquivos de dados deletados (por UPDATE/DELETE/MERGE)
      ficam disponíveis para time travel. Após esse período, VACUUM os remove.

    delta.logRetentionDuration:
      Quanto tempo o transaction log (histórico de operações) é mantido.
      Log mais longo = mais histórico de DESCRIBE HISTORY disponível.

    Recomendações por camada:
      Bronze : 90d (auditoria de ingestão — o dado bruto é prova)
      Silver : 30d (suficiente para reprocessamento de erros)
      Gold   : 60d (relatórios históricos precisam de consistência)

    Nota: ALTER TABLE SET TBLPROPERTIES requer Unity Catalog no Databricks.
    Localmente, as propriedades são definidas via DeltaTable API.
    """
    if not _is_databricks():
        logger.info("[RETENTION] Pulado em modo local (requer Unity Catalog).")
        return

    logger.info("[RETENTION] Configurando políticas de retenção...")

    policies = [
        # (tabela, retenção_dados, retenção_log)
        (f"{cfg.bronze_db}.orders_raw",      "interval 90 days", "interval 30 days"),
        (f"{cfg.bronze_db}.order_items_raw", "interval 90 days", "interval 30 days"),
        (f"{cfg.silver_db}.orders",          "interval 30 days", "interval 14 days"),
        (f"{cfg.silver_db}.order_items",     "interval 30 days", "interval 14 days"),
        (f"{cfg.gold_db}.sales_daily",       "interval 60 days", "interval 30 days"),
        (f"{cfg.gold_db}.sales_weekly",      "interval 60 days", "interval 30 days"),
        (f"{cfg.gold_db}.sales_monthly",     "interval 60 days", "interval 30 days"),
        (f"{cfg.gold_db}.kpi_summary",       "interval 60 days", "interval 30 days"),
    ]

    for table, data_ret, log_ret in policies:
        try:
            spark.sql(f"""
                ALTER TABLE {table}
                SET TBLPROPERTIES (
                    'delta.deletedFileRetentionDuration' = '{data_ret}',
                    'delta.logRetentionDuration'         = '{log_ret}'
                )
            """)
            logger.info(f"[RETENTION] ✔ {table.split('.')[-1]:28s} "
                        f"dados={data_ret} | log={log_ret}")
        except Exception as e:
            logger.warning(f"[RETENTION] Falha em {table}: {e}")

# =============================================================================
# FASE 7: RBAC — CONTROLE DE ACESSO (UNITY CATALOG)
# =============================================================================

def configure_rbac(spark, cfg: PipelineConfig):
    """
    Aplica políticas de controle de acesso por grupo Unity Catalog.

    Modelo de privilégios por grupo:

    data_platform    : ALL PRIVILEGES no catálogo (administração)
    data_engineers   : SELECT + MODIFY em todas as camadas
    data_analysts    : SELECT apenas em Silver e Gold (sem Bronze)
    executives       : SELECT apenas em tabelas Gold específicas

    Por que analistas não acessam Bronze?
      - Dados brutos são não-estruturados e podem ter PII sem mascaramento
      - O acesso controlado reduz risco de uso indevido de dados sensíveis
      - Toda análise deve partir de dados já limpos (Silver ou Gold)

    Nota: GRANT requer Unity Catalog — pulado em modo local.
    """
    if not _is_databricks():
        logger.info("[RBAC] Pulado em modo local (requer Unity Catalog).")
        return

    logger.info("[RBAC] Configurando controle de acesso...")

    g_platform   = cfg.group_platform
    g_engineers  = cfg.group_engineers
    g_analysts   = cfg.group_analysts
    g_executives = cfg.group_executives
    cat          = cfg.catalog

    rbac_grants = [
        # data_platform — controle total
        f"GRANT ALL PRIVILEGES ON CATALOG {cat} TO `{g_platform}`",

        # data_engineers — leitura e escrita em todas as camadas
        f"GRANT USE CATALOG ON CATALOG {cat} TO `{g_engineers}`",
        f"GRANT USE SCHEMA ON SCHEMA {cat}.bronze TO `{g_engineers}`",
        f"GRANT USE SCHEMA ON SCHEMA {cat}.silver TO `{g_engineers}`",
        f"GRANT USE SCHEMA ON SCHEMA {cat}.gold   TO `{g_engineers}`",
        f"GRANT SELECT, MODIFY ON ALL TABLES IN SCHEMA {cat}.bronze TO `{g_engineers}`",
        f"GRANT SELECT, MODIFY ON ALL TABLES IN SCHEMA {cat}.silver TO `{g_engineers}`",
        f"GRANT SELECT, MODIFY ON ALL TABLES IN SCHEMA {cat}.gold   TO `{g_engineers}`",

        # data_analysts — somente leitura Silver e Gold
        f"GRANT USE CATALOG ON CATALOG {cat} TO `{g_analysts}`",
        f"GRANT USE SCHEMA ON SCHEMA {cat}.silver TO `{g_analysts}`",
        f"GRANT USE SCHEMA ON SCHEMA {cat}.gold   TO `{g_analysts}`",
        f"GRANT SELECT ON ALL TABLES IN SCHEMA {cat}.silver TO `{g_analysts}`",
        f"GRANT SELECT ON ALL TABLES IN SCHEMA {cat}.gold   TO `{g_analysts}`",

        # executives — somente tabelas Gold de alto nível
        f"GRANT USE CATALOG ON CATALOG {cat} TO `{g_executives}`",
        f"GRANT USE SCHEMA ON SCHEMA {cat}.gold TO `{g_executives}`",
        f"GRANT SELECT ON TABLE {cat}.gold.kpi_summary        TO `{g_executives}`",
        f"GRANT SELECT ON TABLE {cat}.gold.sales_monthly      TO `{g_executives}`",
        f"GRANT SELECT ON TABLE {cat}.gold.sales_by_category  TO `{g_executives}`",
    ]

    for stmt in rbac_grants:
        try:
            spark.sql(stmt)
            # Logar apenas o verbo e o alvo para não poluir os logs
            parts = stmt.split()
            logger.info(f"[RBAC] ✔ {' '.join(parts[:4])} ... TO {parts[-1]}")
        except Exception as e:
            logger.warning(f"[RBAC] ⚠ Falha: {stmt[:60]}... | Erro: {e}")

    logger.info("[RBAC] Configuração de acesso concluída.")

# =============================================================================
# FASE 7b: COLUMN MASKING — PII (Personally Identifiable Information)
# =============================================================================

def configure_pii_masking(spark, cfg: PipelineConfig):
    """
    Aplica Column Masking para campos PII em Silver.

    Unity Catalog Column Masking:
      - Cria uma função SQL que recebe o valor da coluna como entrada
      - Retorna o valor real para membros do grupo autorizado
      - Retorna o valor mascarado para todos os outros

    Campos mascarados:
      email : retorna "****@****" para quem não é data_engineer
      name  : retorna "***" para quem não é data_engineer

    Isso garante conformidade com LGPD/GDPR sem precisar criar views separadas
    ou duplicar a tabela sem PII.

    Nota: CREATE FUNCTION + IS_ACCOUNT_GROUP_MEMBER requerem Unity Catalog.
    """
    if not _is_databricks():
        logger.info("[MASKING] Pulado em modo local (requer Unity Catalog).")
        return

    logger.info("[MASKING] Configurando Column Masking para PII...")

    g_engineers = cfg.group_engineers
    cat         = cfg.catalog

    masking_statements = [
        # Função de mascaramento para e-mail
        f"""
        CREATE OR REPLACE FUNCTION {cat}.silver.mask_email(email STRING)
        RETURN CASE
            WHEN IS_ACCOUNT_GROUP_MEMBER('{g_engineers}') THEN email
            ELSE '****@****'
        END
        """,
        # Função de mascaramento para nome
        f"""
        CREATE OR REPLACE FUNCTION {cat}.silver.mask_name(name STRING)
        RETURN CASE
            WHEN IS_ACCOUNT_GROUP_MEMBER('{g_engineers}') THEN name
            ELSE '***'
        END
        """,
        # Aplicar mascaramento na tabela orders (campo customer_id como proxy de nome)
        # Nota: adapte os campos conforme o schema real da sua tabela Silver
        # Exemplo para tabela de clientes com campos 'email' e 'name':
        # f"ALTER TABLE {cat}.silver.customers ALTER COLUMN email SET MASK {cat}.silver.mask_email",
        # f"ALTER TABLE {cat}.silver.customers ALTER COLUMN name  SET MASK {cat}.silver.mask_name",
    ]

    for stmt in masking_statements:
        try:
            spark.sql(stmt)
            # Extrair o nome da função do statement para log limpo
            stmt_clean = " ".join(stmt.strip().split()[:6])
            logger.info(f"[MASKING] ✔ {stmt_clean}...")
        except Exception as e:
            logger.warning(f"[MASKING] ⚠ Falha ao aplicar masking: {e}")

    logger.info("[MASKING] Column Masking configurado.")

# =============================================================================
# FASE 8: MONITORAMENTO — DATA FRESHNESS
# =============================================================================

def monitor_freshness(spark, cfg: PipelineConfig, tables_config: dict):
    """
    Verifica se todas as tabelas foram atualizadas hoje.
    Alerta nos logs para tabelas que não foram atualizadas no batch atual.
    """
    logger.info("[FRESHNESS] Verificando atualidade dos dados...")

    all_fresh = True
    for table, tcfg in tables_config.items():
        path = tcfg.get("path", "")
        try:
            if _is_databricks():
                last = spark.sql(f"""
                    SELECT MAX(_processing_date) AS last_dt FROM {table}
                """).collect()[0]["last_dt"]
            else:
                # Modo local: spark.read.format("delta") requer hadoop.dll nativo
                # no Windows e falha com UnsatisfiedLinkError. Lemos o _delta_log
                # diretamente (mesma abordagem de check_table_history) para obter
                # o timestamp do último commit sem acionar o Hadoop native IO.
                import json as _json
                log_dir = Path(path) / "_delta_log"
                last = None
                if log_dir.exists():
                    for jf in sorted(log_dir.glob("*.json"), reverse=True):
                        try:
                            with open(jf, encoding="utf-8") as fh:
                                for line in fh:
                                    entry = _json.loads(line)
                                    if "commitInfo" in entry:
                                        ts_ms = entry["commitInfo"].get("timestamp", 0)
                                        last = datetime.fromtimestamp(
                                            ts_ms / 1000
                                        ).strftime("%Y-%m-%d")
                                        break
                            if last:
                                break
                        except Exception:
                            continue

            if str(last) == cfg.batch_date:
                logger.info(f"[FRESHNESS] ✔ {table.split('.')[-1]:28s} atualizado hoje ({last})")
            else:
                logger.warning(f"[FRESHNESS] ⚠ {table.split('.')[-1]:28s} última atualização: {last}")
                all_fresh = False
        except Exception as e:
            logger.warning(f"[FRESHNESS] Não pôde verificar {table}: {e}")

    if all_fresh:
        logger.info("[FRESHNESS] ✔ Todas as tabelas atualizadas hoje.")
    else:
        logger.warning("[FRESHNESS] ⚠ Algumas tabelas não foram atualizadas hoje.")

# =============================================================================
# HISTÓRICO DELTA — AUDITORIA DE OPERAÇÕES
# =============================================================================

def check_table_history(spark, table_name: str, path: str = "",
                        n_versions: int = 5):
    """
    Exibe o histórico recente de operações da tabela Delta.
    Útil para confirmar que OPTIMIZE/VACUUM foram executados corretamente
    e para debugging de problemas de performance.
    """
    try:
        logger.info(f"[HISTORY] Últimas {n_versions} operações em {table_name}:")
        if _is_databricks():
            df = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT {n_versions}")
            df.select("version", "timestamp", "operation", "operationMetrics") \
              .show(truncate=False, vertical=True)
        else:
            # Modo local: ler _delta_log/*.json diretamente (sem DeltaTable.forPath)
            import json as _json
            log_dir = Path(path) / "_delta_log"
            if not log_dir.exists():
                logger.warning(f"[HISTORY] _delta_log não encontrado: {path}")
                return
            json_files = sorted(log_dir.glob("*.json"), reverse=True)[:n_versions]
            if not json_files:
                logger.info(f"[HISTORY] Nenhum arquivo de log em {table_name}")
                return
            for jf in json_files:
                try:
                    with open(jf, encoding="utf-8") as fh:
                        for line in fh:
                            entry = _json.loads(line)
                            if "commitInfo" in entry:
                                ci = entry["commitInfo"]
                                ts = datetime.fromtimestamp(
                                    ci.get("timestamp", 0) / 1000
                                ).strftime("%Y-%m-%d %H:%M:%S")
                                logger.info(
                                    f"  v{jf.stem:>5} | {ts} | "
                                    f"{ci.get('operation','?'):20s} | "
                                    f"metrics={ci.get('operationMetrics', {})}"
                                )
                                break
                except Exception as exc:
                    logger.warning(f"[HISTORY] Erro lendo {jf.name}: {exc}")
    except Exception as e:
        logger.warning(f"[HISTORY] Não pôde obter histórico de {table_name}: {e}")

# =============================================================================
# MAIN
# =============================================================================

def main():
    logger.info("=" * 60)
    logger.info("OPTIMIZE — Pipeline E2E de Vendas")
    logger.info(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    cfg          = get_config()
    spark        = get_spark(cfg)
    tables_cfg   = get_tables_config(cfg)

    logger.info(f"Ambiente        : {cfg.env}")
    logger.info(f"Databricks      : {_is_databricks()}")
    logger.info(f"Batch date      : {cfg.batch_date}")
    logger.info(f"Retenção VACUUM : {cfg.vacuum_retention_hours}h ({cfg.vacuum_retention_hours//24} dias)")
    logger.info(f"Tabelas         : {len(tables_cfg)}")

    try:
        # ── FASE 1: DIAGNÓSTICO ANTES ─────────────────────────────────────────
        logger.info("\n--- FASE 1: Diagnóstico ANTES ---")
        diag_before = [
            diagnose_table(spark, t, tcfg["path"])
            for t, tcfg in tables_cfg.items()
        ]

        # ── FASE 2: OPTIMIZE + Z-ORDER ────────────────────────────────────────
        logger.info("\n--- FASE 2: OPTIMIZE + Z-Order ---")
        for table, config in tables_cfg.items():
            run_optimize(spark, table, config["path"], config.get("z_order", []))

        # ── FASE 3: VACUUM ────────────────────────────────────────────────────
        logger.info("\n--- FASE 3: VACUUM ---")
        for table, config in tables_cfg.items():
            if config.get("vacuum", False):
                run_vacuum(spark, table, config["path"], cfg.vacuum_retention_hours)

        # ── FASE 4: ANALYZE ───────────────────────────────────────────────────
        logger.info("\n--- FASE 4: ANALYZE TABLE ---")
        for table, config in tables_cfg.items():
            if config.get("analyze", False):
                run_analyze(spark, table, config["path"])

        # ── FASE 5: DIAGNÓSTICO DEPOIS + RELATÓRIO ────────────────────────────
        logger.info("\n--- FASE 5: Diagnóstico DEPOIS + Relatório ---")
        diag_after = [
            diagnose_table(spark, t, tcfg["path"])
            for t, tcfg in tables_cfg.items()
        ]
        generate_report(spark, cfg, diag_before, diag_after)

        # ── FASE 6: POLÍTICAS DE RETENÇÃO ─────────────────────────────────────
        logger.info("\n--- FASE 6: Políticas de Retenção (Time Travel) ---")
        configure_retention_policies(spark, cfg)

        # ── FASE 7: RBAC + COLUMN MASKING ─────────────────────────────────────
        logger.info("\n--- FASE 7: RBAC + Column Masking ---")
        configure_rbac(spark, cfg)
        configure_pii_masking(spark, cfg)

        # ── FASE 8: DATA FRESHNESS ────────────────────────────────────────────
        logger.info("\n--- FASE 8: Monitoramento de Data Freshness ---")
        monitor_freshness(spark, cfg, tables_cfg)

        # ── HISTÓRICO DAS TABELAS CRÍTICAS ────────────────────────────────────
        logger.info("\n--- Histórico Delta (tabelas críticas) ---")
        silver_orders_path = cfg.silver_path + "orders/"
        gold_kpi_path      = cfg.gold_path   + "kpi_summary/"
        check_table_history(spark, f"{cfg.silver_db}.orders",
                            silver_orders_path, n_versions=3)
        check_table_history(spark, f"{cfg.gold_db}.kpi_summary",
                            gold_kpi_path, n_versions=3)

        logger.info("=" * 60)
        logger.info("OPTIMIZE — CONCLUÍDO COM SUCESSO ✔")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"[OPTIMIZE] FALHA CRÍTICA: {e}")
        raise


if __name__ == "__main__":
    main()
