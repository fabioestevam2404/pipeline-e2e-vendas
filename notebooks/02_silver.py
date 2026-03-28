# =============================================================================
# PIPELINE E2E DE VENDAS — CAMADA SILVER
# =============================================================================
# Arquivo  : 02_silver.py
# Propósito: Limpeza, tipagem, deduplicação, validação de regras de negócio,
#            enrichment (join com clientes) e persistência via Delta MERGE.
#
# Fluxo:
#   Bronze (orders + order_items)
#     → Limpeza de strings (trim/lower/initcap)
#     → Conversão de tipos (String → IntegerType, DoubleType, DateType)
#     → Cálculo de campos derivados (subtotal, is_weekend, etc.)
#     → Deduplicação por chave (Window + row_number)
#     → Validações de negócio → registros inválidos → quarentena
#     → Enrichment: join orders × clientes (broadcast)
#     → Delta MERGE (upsert idempotente)
#     → Registro no Unity Catalog
#     → Quality Checks (QualityMetrics)
#     → Linhagem
#
# Dependências: 01_bronze.py (deve ter concluído com sucesso)
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
    from pyspark.sql import SparkSession, DataFrame, Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        IntegerType, DoubleType, DateType, BooleanType, TimestampType
    )
    from delta.tables import DeltaTable
    _PYSPARK_AVAILABLE = True
except ModuleNotFoundError:
    _PYSPARK_AVAILABLE = False

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame
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
        env:            str   = os.environ.get("ENV",           "dev")
        catalog:        str   = os.environ.get("CATALOG",       "pipeline_vendas")
        batch_date:     str   = datetime.now().strftime("%Y-%m-%d")
        bronze_path:    str   = os.environ.get("BRONZE_PATH",   "/mnt/bronze/vendas/")
        silver_path:    str   = os.environ.get("SILVER_PATH",   "/mnt/silver/vendas/")
        audit_path:     str   = os.environ.get("AUDIT_PATH",    "/mnt/audit/pipeline_vendas/")
        rejected_path:  str   = os.environ.get("REJECTED_PATH", "/mnt/silver/vendas/_rejected/")
        min_pass_rate:  float = 0.90
        min_price:      float = 0.01
        max_quantity:   int   = 10_000
        max_discount:   float = 1.0
        @property
        def bronze_db(self): return f"{self.catalog}.bronze"
        @property
        def silver_db(self): return f"{self.catalog}.silver"
        @property
        def tbl_bronze_orders(self): return f"{self.bronze_db}.orders_raw"
        @property
        def tbl_bronze_items(self):  return f"{self.bronze_db}.order_items_raw"
        @property
        def tbl_silver_orders(self): return f"{self.silver_db}.orders"
        @property
        def tbl_silver_items(self):  return f"{self.silver_db}.order_items"
    def get_config(): return PipelineConfig()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("silver")

# =============================================================================
# CLASSE: QUALITY METRICS
# Acumula e reporta métricas de qualidade por tabela.
# Permite rastrear a evolução da qualidade dos dados ao longo do tempo.
# =============================================================================

class QualityMetrics:
    """
    Rastreia métricas de qualidade para uma tabela.

    Uso:
        qm = QualityMetrics("silver.orders")
        qm.record("deduplicacao", total=1000, passed=990)
        qm.assert_pass_rate("deduplicacao", min_rate=0.90)
        ok = qm.summary()   # True se nenhum check crítico falhou
    """

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.metrics    = {}   # check_name → {total, passed, failed, pass_rate}
        self.failures   = []   # lista de checks que falharam o assert

    def record(self, check_name: str, total: int, passed: int):
        pct = (passed / max(total, 1)) * 100
        self.metrics[check_name] = {
            "total":     total,
            "passed":    passed,
            "failed":    total - passed,
            "pass_rate": round(pct, 2),
        }
        status = "✔" if total - passed == 0 else "⚠"
        logger.info(
            f"[QM][{self.table_name}] {status} {check_name}: "
            f"{passed:,}/{total:,} aprovados ({pct:.1f}%)"
        )

    def assert_pass_rate(self, check_name: str, min_rate: float):
        """Registra falha crítica se pass_rate < min_rate."""
        rate = self.metrics.get(check_name, {}).get("pass_rate", 0)
        if rate < min_rate * 100:
            msg = (
                f"{check_name}: {rate:.1f}% < mínimo {min_rate*100:.0f}%"
            )
            self.failures.append(msg)
            logger.error(f"[QM][{self.table_name}] ✘ FALHA CRÍTICA: {msg}")

    def summary(self) -> bool:
        """Exibe resumo e retorna True se não há falhas críticas."""
        sep = "=" * 55
        logger.info(f"\n{sep}\n  QC RESUMO: {self.table_name}\n{sep}")
        for check, v in self.metrics.items():
            icon = "✔" if v["failed"] == 0 else "⚠"
            logger.info(
                f"  [{icon}] {check:<30s} "
                f"{v['pass_rate']:.1f}% "
                f"({v['passed']:,}/{v['total']:,})"
            )
        if self.failures:
            logger.error(f"  FALHAS CRÍTICAS: {self.failures}")
        logger.info(sep)
        return len(self.failures) == 0

# =============================================================================
# SPARK SESSION
# =============================================================================

def _is_databricks() -> bool:
    """Detecta se está rodando dentro do Databricks."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _ensure_hadoop_home() -> None:
    """Configura HADOOP_HOME com winutils.exe para PySpark no Windows."""
    if platform.system() != "Windows":
        return
    hadoop_dir = Path.home() / ".hadoop-winutils"
    bin_dir = hadoop_dir / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    base_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin"
    for fname in ("winutils.exe", "hadoop.dll"):
        dest = bin_dir / fname
        if not dest.exists():
            logger.info(f"[HADOOP] Baixando {fname}...")
            try:
                urllib.request.urlretrieve(f"{base_url}/{fname}", dest)
            except Exception as e:
                logger.warning(f"[HADOOP] Falha ao baixar {fname}: {e}")
    os.environ["HADOOP_HOME"] = str(hadoop_dir)
    os.environ["hadoop.home.dir"] = str(hadoop_dir)
    bin_str = str(bin_dir)
    if bin_str not in os.environ.get("PATH", ""):
        os.environ["PATH"] = bin_str + os.pathsep + os.environ.get("PATH", "")
    logger.info(f"[HADOOP] HADOOP_HOME configurado: {hadoop_dir}")


def _ensure_java_home() -> None:
    """Detecta e configura JAVA_HOME automaticamente se não estiver definido."""
    if os.environ.get("JAVA_HOME"):
        return
    candidates = (
        glob.glob(r"C:\Program Files\Microsoft\jdk-*")
        + glob.glob(r"C:\Program Files\Eclipse Adoptium\jdk-*")
        + glob.glob(r"C:\Program Files\Java\jdk*")
    )
    for path in sorted(candidates, reverse=True):
        if os.path.exists(os.path.join(path, "bin", "java.exe")):
            os.environ["JAVA_HOME"] = path
            logger.info(f"[JAVA] JAVA_HOME detectado automaticamente: {path}")
            return
    logger.warning("[JAVA] Java não encontrado. Instale o JDK e defina JAVA_HOME.")


def get_spark(cfg: "PipelineConfig") -> "SparkSession":
    if not _PYSPARK_AVAILABLE:
        raise RuntimeError(
            "PySpark não está instalado neste ambiente.\n"
            "Para rodar localmente: pip install pyspark delta-spark"
        )
    _ensure_hadoop_home()
    _ensure_java_home()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder
        .appName(f"silver_limpeza_{cfg.env}_{cfg.batch_date}")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled",    "true")
        .config("spark.databricks.delta.autoCompact.enabled",       "true")
        .config("spark.sql.shuffle.partitions",                     "200")
        .config("spark.sql.adaptive.enabled",                        "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled",     "true")
        .config("spark.sql.autoBroadcastJoinThreshold",             str(100 * 1024 * 1024))
    )

    try:
        from delta import configure_spark_with_delta_pip  # type: ignore[import]
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError:
        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

# =============================================================================
# LIMPEZA E TIPAGEM — ORDERS
# =============================================================================

def clean_orders(df_raw: DataFrame, cfg: PipelineConfig):
    """
    Limpa e tipora a tabela de orders.

    Etapas:
      1. Padronização de strings (trim, lower, initcap)
      2. Conversão de tipos (String → Date, Double)
      3. Campos derivados (order_year, order_month, is_weekend, order_segment)
      4. Deduplicação por order_id (mantém registro mais recente)
      5. Validação de campos obrigatórios
      6. Quarentena de inválidos
    """
    logger.info("[SILVER] Limpeza: Orders")
    qm    = QualityMetrics(cfg.tbl_silver_orders)
    total = df_raw.count()

    # ── 1. Padronização de strings ────────────────────────────────────────────
    df = (
        df_raw
        .withColumn("order_id",    F.trim(F.col("order_id")))
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("status",      F.upper(F.trim(F.col("status"))))
        .withColumn("channel",     F.lower(F.trim(F.col("channel"))))
        .withColumn("region",      F.lower(F.trim(F.col("region"))))
        .withColumn("currency",    F.upper(F.trim(F.col("currency"))))
    )

    # ── 2. Conversão de tipos ─────────────────────────────────────────────────
    df = (
        df
        .withColumn("order_date",
                    F.to_date(F.col("order_date"), "yyyy-MM-dd"))
        .withColumn("total_amount",
                    F.col("total_amount").cast(DoubleType()))
    )

    # ── 3. Campos derivados ───────────────────────────────────────────────────
    df = (
        df
        .withColumn("order_year",       F.year("order_date"))
        .withColumn("order_month",      F.month("order_date"))
        .withColumn("order_week",       F.weekofyear("order_date"))
        .withColumn("order_dayofweek",  F.dayofweek("order_date"))
        # is_weekend: dayofweek 1=Domingo, 7=Sábado
        .withColumn("is_weekend",
                    F.col("order_dayofweek").isin(1, 7))
        # Segmentação de pedidos por valor total
        .withColumn("order_segment",
                    F.when(F.col("total_amount") >= 1000, "alto_valor")
                     .when(F.col("total_amount") >= 200,  "medio_valor")
                     .otherwise("baixo_valor"))
        # Flag de validade
        .withColumn("_is_valid", F.lit(True))
    )

    # ── 4. Deduplicação por order_id ──────────────────────────────────────────
    # Estratégia: manter o registro com _ingestion_timestamp mais recente.
    # Isso trata reenvios de arquivos e reprocessamentos sem criar duplicatas.
    window_dedup = (
        Window
        .partitionBy("order_id")
        .orderBy(F.desc("_ingestion_timestamp"))
    )
    df = (
        df
        .withColumn("_row_num", F.row_number().over(window_dedup))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    after_dedup = df.count()
    qm.record("deduplicacao", total, after_dedup)

    # ── 5. Validação de campos obrigatórios ───────────────────────────────────
    df_valid = df.filter(
        F.col("order_id").isNotNull() &
        F.col("customer_id").isNotNull() &
        F.col("order_date").isNotNull() &
        (F.col("status") != "") &
        F.col("order_year").isNotNull()
    )
    qm.record("campos_obrigatorios", after_dedup, df_valid.count())
    qm.assert_pass_rate("campos_obrigatorios", min_rate=cfg.min_pass_rate)

    # ── 6. Quarentena de inválidos ────────────────────────────────────────────
    n_rejected = after_dedup - df_valid.count()
    if n_rejected > 0:
        (
            df.subtract(df_valid)
            .withColumn("_rejection_reason",    F.lit("missing_required_fields"))
            .withColumn("_rejection_timestamp", F.current_timestamp())
            .write.format("delta").mode("append")
            .save(cfg.rejected_path + "orders/")
        )
        logger.warning(f"[SILVER] {n_rejected:,} orders rejeitados → quarentena")

    # ── 7. Metadados Silver ───────────────────────────────────────────────────
    df_silver = (
        df_valid
        .withColumn("_silver_timestamp", F.current_timestamp())
        .withColumn("_silver_date",      F.current_date())
        .withColumn("_batch_id",         F.lit(cfg.batch_date))
    )

    qm.summary()
    return df_silver, qm

# =============================================================================
# LIMPEZA E TIPAGEM — ORDER ITEMS
# =============================================================================

def clean_order_items(df_raw: DataFrame, cfg: PipelineConfig):
    """
    Limpa e tipora a tabela de order_items.

    Validações de negócio aplicadas aqui:
      - Quantidade: 0 < qty ≤ max_quantity
      - Preço unitário: >= min_price
      - Desconto: entre 0 e max_discount (0% a 100%)
      - Subtotal calculado: quantidade × preço × (1 - desconto)
    """
    logger.info("[SILVER] Limpeza: Order Items")
    qm    = QualityMetrics(cfg.tbl_silver_items)
    total = df_raw.count()

    # ── 1. Padronização ───────────────────────────────────────────────────────
    df = (
        df_raw
        .withColumn("item_id",      F.trim(F.col("item_id")))
        .withColumn("order_id",     F.trim(F.col("order_id")))
        .withColumn("product_id",   F.trim(F.col("product_id")))
        .withColumn("product_name", F.initcap(F.trim(F.col("product_name"))))
        .withColumn("category",     F.lower(F.trim(F.col("category"))))
    )

    # ── 2. Conversão de tipos ─────────────────────────────────────────────────
    df = (
        df
        .withColumn("quantity",   F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
        .withColumn("discount",
                    F.coalesce(F.col("discount").cast(DoubleType()), F.lit(0.0)))
    )

    # ── 3. Subtotal calculado ─────────────────────────────────────────────────
    df = df.withColumn(
        "subtotal",
        F.round(
            F.col("quantity") * F.col("unit_price") * (F.lit(1.0) - F.col("discount")),
            2
        )
    )

    # ── 4. Deduplicação ───────────────────────────────────────────────────────
    window_dedup = (
        Window
        .partitionBy("item_id")
        .orderBy(F.desc("_ingestion_timestamp"))
    )
    df = (
        df
        .withColumn("_row_num", F.row_number().over(window_dedup))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    after_dedup = df.count()
    qm.record("deduplicacao", total, after_dedup)

    # ── 5. Validações de regras de negócio ────────────────────────────────────
    df_valid = df.filter(
        F.col("item_id").isNotNull() &
        F.col("order_id").isNotNull() &
        F.col("product_id").isNotNull() &
        (F.col("quantity") > 0) &
        (F.col("quantity") <= cfg.max_quantity) &
        (F.col("unit_price") >= cfg.min_price) &
        F.col("discount").between(0, cfg.max_discount) &
        (F.col("subtotal") > 0)
    )
    n_valid = df_valid.count()
    qm.record("validacao_negocio", after_dedup, n_valid)
    qm.assert_pass_rate("validacao_negocio", min_rate=cfg.min_pass_rate)

    # ── 6. Quarentena ─────────────────────────────────────────────────────────
    n_rejected = after_dedup - n_valid
    if n_rejected > 0:
        (
            df.subtract(df_valid)
            .withColumn("_rejection_reason",    F.lit("business_rule_violation"))
            .withColumn("_rejection_timestamp", F.current_timestamp())
            .write.format("delta").mode("append")
            .save(cfg.rejected_path + "order_items/")
        )
        logger.warning(f"[SILVER] {n_rejected:,} items rejeitados → quarentena")

    # ── 7. Flag de validade + metadados ──────────────────────────────────────
    df_silver = (
        df_valid
        .withColumn("_is_valid",         F.lit(True))
        .withColumn("_silver_timestamp", F.current_timestamp())
        .withColumn("_silver_date",      F.current_date())
        .withColumn("_batch_id",         F.lit(cfg.batch_date))
    )

    qm.summary()
    return df_silver, qm

# =============================================================================
# DELTA MERGE — UPSERT IDEMPOTENTE
# =============================================================================

def delta_merge(spark, df: DataFrame, table_path: str,
                table_name: str, merge_key: str):
    """
    Persiste dados via Delta MERGE (upsert).

    Comportamento:
      - Se o registro com a chave já existe → ATUALIZA todos os campos
      - Se o registro não existe → INSERE

    Por que usar MERGE em vez de overwrite?
      - Idempotente: executar duas vezes não duplica dados
      - Incremental: processa apenas deltas, não a tabela inteira
      - Seguro: mantém dados de períodos anteriores intactos

    Para a primeira carga, faz write simples (tabela ainda não existe).
    """
    logger.info(f"[SILVER] MERGE: {table_name} (chave: {merge_key})")
    try:
        delta_table = DeltaTable.forPath(spark, table_path)
        (
            delta_table.alias("target")
            .merge(
                df.alias("source"),
                f"target.{merge_key} = source.{merge_key}"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(f"[SILVER] ✔ MERGE concluído: {table_name}")

    except Exception:
        # Tabela não existe ainda → cria com write inicial
        logger.info(f"[SILVER] Tabela nova — criando: {table_name}")
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(table_path)
        )
        logger.info(f"[SILVER] ✔ Tabela criada: {table_name}")


def save_orders_silver(df: DataFrame, cfg: PipelineConfig):
    """
    Salva orders com overwrite particionado por batch_date.

    Usa replaceWhere para substituir apenas a partição do dia atual,
    preservando dados de dias anteriores.
    """
    logger.info("[SILVER] Salvando orders (overwrite particionado)...")

    path = cfg.silver_path + "orders/"

    try:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere",    f"_silver_date = '{cfg.batch_date}'")
            .option("overwriteSchema", "true")
            .partitionBy("order_year", "order_month")
            .save(path)
        )
    except Exception:
        # Primeira carga: partição não existe ainda
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("order_year", "order_month")
            .save(path)
        )
    logger.info(f"[SILVER] ✔ Orders salvas: {path}")


def save_items_silver(df: DataFrame, cfg: PipelineConfig):
    """
    Salva order_items com overwrite particionado.
    Partição por order_year e order_month (via join com orders) quando disponível,
    ou por _batch_id como fallback.
    """
    logger.info("[SILVER] Salvando order_items (overwrite particionado)...")
    path = cfg.silver_path + "order_items/"

    # Adicionar coluna de partição baseada em order_month extraída do batch_date
    batch_dt = datetime.strptime(cfg.batch_date, "%Y-%m-%d")
    df = (
        df
        .withColumn("part_year",  F.lit(batch_dt.year))
        .withColumn("part_month", F.lit(batch_dt.month))
    )

    try:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere",    f"_batch_id = '{cfg.batch_date}'")
            .option("overwriteSchema", "true")
            .partitionBy("part_year", "part_month")
            .save(path)
        )
    except Exception:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("part_year", "part_month")
            .save(path)
        )
    logger.info(f"[SILVER] ✔ Items salvos: {path}")

# =============================================================================
# REGISTRO NO UNITY CATALOG
# =============================================================================

def register_silver_tables(spark, cfg: "PipelineConfig"):
    """Registra tabelas Delta no Unity Catalog. Ignorado fora do Databricks."""
    if not _is_databricks():
        logger.info("[CATALOG] Registro ignorado (execução local).")
        return
    logger.info("[SILVER] Registrando tabelas no Unity Catalog...")

    tables = [
        (
            cfg.tbl_silver_orders,
            cfg.silver_path + "orders/",
            "Orders de vendas: limpos, deduplicados e com campos derivados"
        ),
        (
            cfg.tbl_silver_items,
            cfg.silver_path + "order_items/",
            "Itens de pedido: limpos, validados e com subtotal calculado"
        ),
    ]
    for name, path, comment in tables:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {name}
            USING DELTA
            LOCATION '{path}'
            COMMENT '{comment}'
        """)
        spark.sql(f"""
            ALTER TABLE {name}
            SET TBLPROPERTIES (
                'layer'                          = 'silver',
                'pipeline'                       = 'pipeline_vendas',
                'update_frequency'               = 'daily',
                'delta.enableChangeDataFeed'     = 'true'
            )
        """)
        logger.info(f"[CATALOG] ✔ Registrado: {name}")

# =============================================================================
# LINHAGEM
# =============================================================================

def log_lineage(spark, cfg: PipelineConfig, source: str,
                target: str, count: int, status: str = "SUCCESS"):
    try:
        record = spark.createDataFrame([{
            "pipeline":     "02_silver",
            "env":          cfg.env,
            "source":       source,
            "target":       target,
            "record_count": count,
            "batch_date":   cfg.batch_date,
            "status":       status,
            "timestamp":    datetime.now().isoformat(),
        }])
        record.write.format("delta").mode("append").save(cfg.audit_path)
        logger.info(f"[LINEAGE] {source} → {target} | {count:,} | {status}")
    except Exception as e:
        logger.warning(f"[LINEAGE] Falha: {e}")

# =============================================================================
# MAIN
# =============================================================================

def main():
    logger.info("=" * 60)
    logger.info("SILVER — Pipeline E2E de Vendas")
    logger.info(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    cfg   = get_config()
    spark = get_spark(cfg)

    logger.info(f"Ambiente   : {cfg.env}")
    logger.info(f"Batch date : {cfg.batch_date}")

    try:
        # ── 1. CARREGAR BRONZE ────────────────────────────────────────────────
        logger.info("[SILVER] Carregando Bronze...")
        df_orders_raw = spark.read.format("delta").load(
            cfg.bronze_path + "orders/"
        )
        df_items_raw = spark.read.format("delta").load(
            cfg.bronze_path + "order_items/"
        )
        logger.info(
            f"[SILVER] Bronze carregado — "
            f"orders: {df_orders_raw.count():,} | "
            f"items: {df_items_raw.count():,}"
        )

        # ── 2. LIMPEZA ────────────────────────────────────────────────────────
        df_orders_silver, qm_orders = clean_orders(df_orders_raw, cfg)
        df_items_silver,  qm_items  = clean_order_items(df_items_raw, cfg)

        # ── 3. PERSISTÊNCIA ───────────────────────────────────────────────────
        save_orders_silver(df_orders_silver, cfg)
        save_items_silver(df_items_silver, cfg)

        cnt_orders = df_orders_silver.count()
        cnt_items  = df_items_silver.count()

        # ── 4. REGISTRO NO CATÁLOGO ───────────────────────────────────────────
        register_silver_tables(spark, cfg)

        # ── 5. QUALITY CHECKS — VALIDAÇÃO FINAL ──────────────────────────────
        ok = qm_orders.summary() and qm_items.summary()
        if not ok:
            raise Exception("Quality checks Silver falharam. Verifique os logs.")

        # ── 6. LINHAGEM ───────────────────────────────────────────────────────
        log_lineage(spark, cfg,
                    cfg.tbl_bronze_orders, cfg.tbl_silver_orders, cnt_orders)
        log_lineage(spark, cfg,
                    cfg.tbl_bronze_items,  cfg.tbl_silver_items,  cnt_items)

        logger.info("=" * 60)
        logger.info("SILVER — CONCLUÍDO COM SUCESSO ✔")
        logger.info(f"  Orders Silver : {cnt_orders:,}")
        logger.info(f"  Items Silver  : {cnt_items:,}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"[SILVER] FALHA CRÍTICA: {e}")
        log_lineage(spark, cfg,
                    cfg.tbl_bronze_orders, cfg.tbl_silver_orders, 0, "FAILED")
        raise


if __name__ == "__main__":
    main()
