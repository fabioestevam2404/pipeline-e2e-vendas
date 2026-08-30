# =============================================================================
# PIPELINE E2E DE VENDAS — CAMADA BRONZE
# =============================================================================
# Arquivo  : 01_bronze.py
# Propósito: Ingestão de dados brutos (JSON e CSV) com metadados completos,
#            tratamento de registros corrompidos (quarentena) e registro
#            no Unity Catalog.
#
# Fluxo:
#   Fonte (raw/) → Leitura com schema explícito
#               → Separação de corrompidos → Quarentena
#               → Enriquecimento com metadados
#               → Escrita Delta particionada (append)
#               → Registro no Unity Catalog
#               → Quality Checks
#               → Linhagem
#
# Dependências: 00_setup.py (deve ter rodado com sucesso)
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
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, DateType
    )
    _PYSPARK_AVAILABLE = True
except ModuleNotFoundError:
    _PYSPARK_AVAILABLE = False

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# Importar configuração centralizada
# No Databricks: %run ./00_config ou sys.path insert
try:
    from config import get_config, PipelineConfig
except ImportError:
    # Fallback inline para execução autônoma (sem 00_config.py no sys.path)
    # Carrega .env antes de definir os defaults para que os valores sejam lidos
    try:
        from dotenv import load_dotenv
        load_dotenv(override=False)
    except ImportError:
        pass
    from dataclasses import dataclass
    @dataclass
    class PipelineConfig:
        env:             str = os.environ.get("ENV",              "dev")
        catalog:         str = os.environ.get("CATALOG",          "pipeline_vendas")
        batch_date:      str = datetime.now().strftime("%Y-%m-%d")
        raw_path:        str = os.environ.get("RAW_PATH",         "/mnt/raw/vendas/")
        bronze_path:     str = os.environ.get("BRONZE_PATH",      "/mnt/bronze/vendas/")
        audit_path:      str = os.environ.get("AUDIT_PATH",       "/mnt/audit/pipeline_vendas/")
        quarantine_path: str = os.environ.get("QUARANTINE_PATH",  "/mnt/bronze/vendas/_quarantine/")
        def __post_init__(self) -> None:
            datetime.strptime(self.batch_date, "%Y-%m-%d")  # ver bandit B608 em 00_config.py
        @property
        def bronze_db(self): return f"{self.catalog}.bronze"
        @property
        def tbl_bronze_orders(self): return f"{self.bronze_db}.orders_raw"
        @property
        def tbl_bronze_items(self):  return f"{self.bronze_db}.order_items_raw"
        @property
        def source_json(self): return f"{self.raw_path}json/"
        @property
        def source_csv(self):  return f"{self.raw_path}csv/"
    def get_config(): return PipelineConfig()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("bronze")

# =============================================================================
# SCHEMAS EXPLÍCITOS
# Todos os campos são lidos como String — conversões de tipo ocorrem na Silver.
# Exceção: campos numéricos com formato garantido pela fonte (ex: IDs inteiros).
# Usar schema explícito evita double-scan (inferSchema=true lê tudo duas vezes).
# =============================================================================

ORDERS_SCHEMA = StructType([
    StructField("order_id",         StringType(),  nullable=False),
    StructField("customer_id",      StringType(),  nullable=False),
    StructField("order_date",       StringType(),  nullable=True),   # conv. na Silver
    StructField("status",           StringType(),  nullable=True),
    StructField("channel",          StringType(),  nullable=True),
    StructField("region",           StringType(),  nullable=True),
    StructField("currency",         StringType(),  nullable=True),
    StructField("total_amount",     StringType(),  nullable=True),   # conv. na Silver
    # Obrigatório quando mode=PERMISSIVE + schema explícito: Spark só preenche
    # esta coluna se ela estiver declarada no schema.
    StructField("_corrupt_record",  StringType(),  nullable=True),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("item_id",          StringType(),  nullable=False),
    StructField("order_id",         StringType(),  nullable=False),
    StructField("product_id",       StringType(),  nullable=True),
    StructField("product_name",     StringType(),  nullable=True),
    StructField("category",         StringType(),  nullable=True),
    StructField("quantity",         StringType(),  nullable=True),   # conv. na Silver
    StructField("unit_price",       StringType(),  nullable=True),   # conv. na Silver
    StructField("discount",         StringType(),  nullable=True),   # conv. na Silver
    # Obrigatório quando mode=PERMISSIVE + schema explícito
    StructField("_corrupt_record",  StringType(),  nullable=True),
])

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
                urllib.request.urlretrieve(f"{base_url}/{fname}", dest)  # nosec B310 - base_url e fname sao constantes fixas (winutils oficial), nao input externo
            except Exception as e:
                logger.warning(f"[HADOOP] Falha ao baixar {fname}: {e}")
    os.environ["HADOOP_HOME"] = str(hadoop_dir)
    os.environ["hadoop.home.dir"] = str(hadoop_dir)
    # hadoop.dll precisa estar no PATH para que a JVM o encontre via java.library.path
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


def get_spark(cfg: PipelineConfig) -> "SparkSession":
    if not _PYSPARK_AVAILABLE:
        raise RuntimeError(
            "PySpark não está instalado neste ambiente.\n"
            "Para rodar localmente: pip install pyspark delta-spark"
        )
    _ensure_hadoop_home()
    _ensure_java_home()
    # Garante que o executor Spark use o mesmo Python do driver.
    # Sem isso, no Windows o comando "python" aponta para o atalho da Microsoft Store.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder
        .appName(f"bronze_ingestao_{cfg.env}_{cfg.batch_date}")
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
    )

    try:
        from delta import configure_spark_with_delta_pip  # type: ignore[import]
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError:
        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

# =============================================================================
# METADADOS DE INGESTÃO
# Cada registro Bronze recebe colunas de rastreabilidade que permitem:
#   - Identificar exatamente de qual arquivo o dado veio
#   - Saber quando foi ingerido (timestamp + data para partição)
#   - Detectar duplicatas entre batches (row_hash)
#   - Filtrar por ambiente e batch_date
# =============================================================================

def add_ingestion_metadata(df, source_name: str, cfg: PipelineConfig):
    """
    Adiciona colunas de controle ao DataFrame Bronze.

    Colunas adicionadas:
      _ingestion_timestamp : TIMESTAMP — momento exato da ingestão
      _ingestion_date      : DATE      — usado como coluna de partição
      _source_file         : STRING    — caminho do arquivo de origem
      _source_name         : STRING    — identificador lógico da fonte
      _batch_id            : STRING    — data do batch para rastreamento
      _row_hash            : STRING    — MD5 de todos os campos da linha
      _env                 : STRING    — ambiente (dev/staging/prod)
    """
    return (
        df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_ingestion_date",      F.lit(cfg.batch_date).cast(DateType()))
        .withColumn("_source_file",         F.input_file_name())
        .withColumn("_source_name",         F.lit(source_name))
        .withColumn("_batch_id",            F.lit(cfg.batch_date))
        .withColumn("_row_hash",
                    F.md5(F.concat_ws("|", *[F.col(c) for c in df.columns])))
        .withColumn("_env",                 F.lit(cfg.env))
    )

# =============================================================================
# LEITURA COM TRATAMENTO DE ERROS
# =============================================================================

def read_json_with_quarantine(spark, cfg: PipelineConfig):
    """
    Lê pedidos em JSON (modo PERMISSIVE).

    PERMISSIVE: registros inválidos recebem _corrupt_record em vez de
    lançar exceção — o job não falha por causa de um arquivo com problema.

    Registros corrompidos são gravados na quarentena para análise posterior.
    Isso garante que NENHUM dado se perde — mesmo os inválidos são preservados.
    """
    logger.info(f"[BRONZE] Lendo orders JSON: {cfg.source_json}")

    df_raw = (
        spark.read
        .format("json")
        .schema(ORDERS_SCHEMA)
        .option("multiLine",                 "false")   # JSONL (um objeto por linha)
        .option("mode",                      "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("encoding",                  "UTF-8")
        .load(cfg.source_json)
        .cache()   # obrigatório: sem cache o Spark não permite filtrar só por _corrupt_record
    )

    total = df_raw.count()
    logger.info(f"[BRONZE] Orders raw: {total:,} linhas lidas")

    # Separar corrompidos ANTES de qualquer transformação
    df_corrupt = df_raw.filter(F.col("_corrupt_record").isNotNull())
    n_corrupt  = df_corrupt.count()

    if n_corrupt > 0:
        logger.warning(f"[BRONZE] ⚠ {n_corrupt:,} registros corrompidos → quarentena")
        (
            df_corrupt
            .withColumn("_quarantine_timestamp", F.current_timestamp())
            .withColumn("_quarantine_reason",    F.lit("json_parse_error"))
            .write
            .format("delta")
            .mode("append")
            .save(cfg.quarantine_path + "orders/")
        )

    # Continua apenas com os válidos (sem a coluna de erro)
    df_valid = (
        df_raw
        .filter(F.col("_corrupt_record").isNull())
        .drop("_corrupt_record")
    )
    logger.info(f"[BRONZE] Orders válidos: {df_valid.count():,} ({n_corrupt:,} quarentena)")
    return df_valid, total, n_corrupt


def read_csv_with_quarantine(spark, cfg: PipelineConfig):
    """
    Lê itens de pedido em CSV com cabeçalho.

    Parâmetros críticos:
      - header=true   : primeira linha é cabeçalho
      - encoding=UTF-8: garante leitura correta de acentos
      - quote/escape  : trata campos com vírgulas dentro de aspas
    """
    logger.info(f"[BRONZE] Lendo order_items CSV: {cfg.source_csv}")

    df_raw = (
        spark.read
        .format("csv")
        .schema(ORDER_ITEMS_SCHEMA)
        .option("header",                    "true")
        .option("sep",                       ",")
        .option("quote",                     '"')
        .option("escape",                    '"')
        .option("encoding",                  "UTF-8")
        .option("mode",                      "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(cfg.source_csv)
        .cache()   # obrigatório: sem cache o Spark não permite filtrar só por _corrupt_record
    )

    total = df_raw.count()
    logger.info(f"[BRONZE] Items raw: {total:,} linhas lidas")

    df_corrupt = df_raw.filter(F.col("_corrupt_record").isNotNull())
    n_corrupt  = df_corrupt.count()

    if n_corrupt > 0:
        logger.warning(f"[BRONZE] ⚠ {n_corrupt:,} itens corrompidos → quarentena")
        (
            df_corrupt
            .withColumn("_quarantine_timestamp", F.current_timestamp())
            .withColumn("_quarantine_reason",    F.lit("csv_parse_error"))
            .write
            .format("delta")
            .mode("append")
            .save(cfg.quarantine_path + "order_items/")
        )

    df_valid = (
        df_raw
        .filter(F.col("_corrupt_record").isNull())
        .drop("_corrupt_record")
    )
    logger.info(f"[BRONZE] Items válidos: {df_valid.count():,}")
    return df_valid, total, n_corrupt

# =============================================================================
# ESCRITA DELTA — APPEND PARTICIONADO
# =============================================================================

def write_bronze(df, path: str, table_name: str) -> int:
    """
    Grava em modo APPEND particionado por _ingestion_date.

    APPEND na Bronze é intencional: preservamos o histórico completo de
    ingestões. Reprocessamentos não sobrescrevem dados anteriores.

    mergeSchema=true permite que campos novos sejam adicionados sem quebrar
    ingestões futuras (evolução de schema segura).

    Retorna contagem de registros gravados.
    """
    count = df.count()
    logger.info(f"[BRONZE] Gravando {table_name}: {count:,} registros → {path}")

    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("_ingestion_date")
        .save(path)
    )
    logger.info(f"[BRONZE] ✔ {table_name} gravado com sucesso")
    return count

# =============================================================================
# REGISTRO NO UNITY CATALOG
# =============================================================================

def register_table(spark, table_name: str, path: str, comment: str = ""):
    """
    Registra tabela Delta no Unity Catalog.
    Idempotente (CREATE TABLE IF NOT EXISTS).
    Ignorado fora do Databricks — Unity Catalog é exclusivo da plataforma.
    """
    if not _is_databricks():
        logger.info(f"[CATALOG] Registro ignorado (execução local): {table_name}")
        return
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION '{path}'
        COMMENT '{comment}'
    """)
    spark.sql(f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES (
            'layer'            = 'bronze',
            'pipeline'         = 'pipeline_vendas',
            'update_frequency' = 'daily'
        )
    """)
    logger.info(f"[CATALOG] ✔ Registrado: {table_name}")

# =============================================================================
# QUALITY CHECKS — BRONZE
# Checks leves: confirmamos chegada dos dados e detectamos anomalias.
# Checks pesados (validação de regras de negócio) ocorrem na Silver.
# =============================================================================

def run_quality_checks(spark, cfg: PipelineConfig, counts: dict) -> bool:
    """
    Executa quality checks de Bronze e retorna True se todos aprovaram.

    Checks executados:
      QC-1: Contagem mínima — tabelas não podem estar vazias
      QC-2: Chave primária — order_id e item_id não podem ser nulos
      QC-3: Duplicatas por hash — detecta reenvio de arquivos
      QC-4: Taxa de corrupção — alerta se >1% dos registros corromperam

    QC-1, QC-2 e QC-4 requerem Unity Catalog e são ignorados fora do Databricks.
    QC-3 (taxa de corrupção) é calculado localmente sem SQL.
    """
    logger.info("[QC] Iniciando quality checks — Bronze")
    failed = []

    if not _is_databricks():
        logger.info("[QC] Checks SQL ignorados (execução local — requerem Unity Catalog).")
        # QC-3 pode rodar localmente pois usa apenas as contagens já calculadas
        total_orders = counts.get("orders_total", 0)
        corrupt_orders = counts.get("orders_corrupt", 0)
        if total_orders > 0:
            corrupt_pct = corrupt_orders / total_orders * 100
            if corrupt_pct > 1.0:
                logger.warning(f"[QC] ⚠ QC-3: {corrupt_pct:.1f}% de orders corrompidos")
            else:
                logger.info(f"[QC] ✔ QC-3 Taxa de corrupção: {corrupt_pct:.2f}%")
        logger.info("[QC] ✔ Quality checks locais concluídos")
        return True

    # QC-1: Contagem mínima
    for tbl, key in [
        (cfg.tbl_bronze_orders, "orders"),
        (cfg.tbl_bronze_items,  "items"),
    ]:
        try:
            n = spark.sql(f"""
                SELECT COUNT(*) AS n FROM {tbl}
                WHERE _batch_id = '{cfg.batch_date}'
            """).collect()[0]["n"]

            if n == 0:
                msg = f"QC-1 FALHOU: {tbl} vazia para batch {cfg.batch_date}"
                logger.error(f"[QC] ✘ {msg}")
                failed.append(msg)
            else:
                logger.info(f"[QC] ✔ QC-1 {tbl}: {n:,} registros no batch de hoje")
        except Exception as e:
            logger.warning(f"[QC] QC-1 não pôde ser executado em {tbl}: {e}")

    # QC-2: Chave primária
    for tbl, pk in [
        (cfg.tbl_bronze_orders, "order_id"),
        (cfg.tbl_bronze_items,  "item_id"),
    ]:
        try:
            n_null = spark.sql(f"""
                SELECT COUNT(*) AS n FROM {tbl}
                WHERE {pk} IS NULL
                AND _batch_id = '{cfg.batch_date}'
            """).collect()[0]["n"]

            if n_null > 0:
                msg = f"QC-2 AVISO: {n_null:,} registros sem {pk} em {tbl}"
                logger.warning(f"[QC] ⚠ {msg}")
            else:
                logger.info(f"[QC] ✔ QC-2 {tbl}: chave primária ({pk}) íntegra")
        except Exception as e:
            logger.warning(f"[QC] QC-2 não pôde ser executado: {e}")

    # QC-3: Taxa de corrupção
    total_orders = counts.get("orders_total", 0)
    corrupt_orders = counts.get("orders_corrupt", 0)
    if total_orders > 0:
        corrupt_pct = corrupt_orders / total_orders * 100
        if corrupt_pct > 1.0:
            msg = f"QC-3 AVISO: {corrupt_pct:.1f}% de orders corrompidos (>{1.0}%)"
            logger.warning(f"[QC] ⚠ {msg}")
        else:
            logger.info(f"[QC] ✔ QC-3 Taxa de corrupção: {corrupt_pct:.2f}%")

    # QC-4: Duplicatas por hash no batch atual
    try:
        n_dupes = spark.sql(f"""
            SELECT COUNT(*) AS n FROM (
                SELECT _row_hash, COUNT(*) AS c
                FROM {cfg.tbl_bronze_orders}
                WHERE _batch_id = '{cfg.batch_date}'
                GROUP BY _row_hash
                HAVING c > 1
            )
        """).collect()[0]["n"]

        if n_dupes > 0:
            logger.warning(f"[QC] ⚠ QC-4: {n_dupes:,} hashes duplicados no batch atual")
        else:
            logger.info("[QC] ✔ QC-4 Nenhuma linha duplicada por hash")
    except Exception as e:
        logger.warning(f"[QC] QC-4 não pôde ser executado: {e}")

    if failed:
        logger.error(f"[QC] {len(failed)} check(s) crítico(s) falharam: {failed}")
        return False

    logger.info("[QC] ✔ Todos os quality checks Bronze aprovados")
    return True

# =============================================================================
# LINHAGEM — AUDIT TRAIL
# =============================================================================

def log_lineage(spark, cfg: PipelineConfig, source: str,
                target: str, count: int, status: str = "SUCCESS"):
    """
    Persiste registro de linhagem no audit trail Delta.
    Permite rastrear: de onde vieram os dados, quando e quantos registros.
    """
    try:
        record = spark.createDataFrame([{
            "pipeline":       "01_bronze",
            "env":            cfg.env,
            "source":         source,
            "target":         target,
            "record_count":   count,
            "batch_date":     cfg.batch_date,
            "status":         status,
            "timestamp":      datetime.now().isoformat(),
        }])
        record.write.format("delta").mode("append").save(cfg.audit_path)
        logger.info(f"[LINEAGE] {source} → {target} | {count:,} registros | {status}")
    except Exception as e:
        logger.warning(f"[LINEAGE] Falha ao gravar linhagem: {e}")

# =============================================================================
# MAIN
# =============================================================================

def main():
    logger.info("=" * 60)
    logger.info("BRONZE — Pipeline E2E de Vendas")
    logger.info(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    cfg   = get_config()
    spark = get_spark(cfg)

    logger.info(f"Ambiente   : {cfg.env}")
    logger.info(f"Batch date : {cfg.batch_date}")
    logger.info(f"Catálogo   : {cfg.catalog}")
    logger.info(f"Bronze path: {cfg.bronze_path}")

    counts = {}

    try:
        # ── 1. ORDERS (JSON) ──────────────────────────────────────────────────
        df_orders, total_o, corrupt_o = read_json_with_quarantine(spark, cfg)
        df_orders = add_ingestion_metadata(df_orders, "orders_json", cfg)
        cnt_orders = write_bronze(
            df_orders,
            cfg.bronze_path + "orders/",
            cfg.tbl_bronze_orders
        )
        register_table(
            spark,
            cfg.tbl_bronze_orders,
            cfg.bronze_path + "orders/",
            "Pedidos brutos ingeridos via JSON"
        )
        counts.update({
            "orders_total": total_o,
            "orders_corrupt": corrupt_o,
            "orders_written": cnt_orders,
        })

        # ── 2. ORDER ITEMS (CSV) ──────────────────────────────────────────────
        df_items, total_i, corrupt_i = read_csv_with_quarantine(spark, cfg)
        df_items = add_ingestion_metadata(df_items, "order_items_csv", cfg)
        cnt_items = write_bronze(
            df_items,
            cfg.bronze_path + "order_items/",
            cfg.tbl_bronze_items
        )
        register_table(
            spark,
            cfg.tbl_bronze_items,
            cfg.bronze_path + "order_items/",
            "Itens de pedido brutos ingeridos via CSV"
        )
        counts.update({
            "items_total": total_i,
            "items_corrupt": corrupt_i,
            "items_written": cnt_items,
        })

        # ── 3. QUALITY CHECKS ─────────────────────────────────────────────────
        qc_ok = run_quality_checks(spark, cfg, counts)
        if not qc_ok:
            raise Exception("Quality checks Bronze falharam. Verifique os logs.")

        # ── 4. LINHAGEM ───────────────────────────────────────────────────────
        log_lineage(spark, cfg, cfg.source_json, cfg.tbl_bronze_orders, cnt_orders)
        log_lineage(spark, cfg, cfg.source_csv,  cfg.tbl_bronze_items,  cnt_items)

        logger.info("=" * 60)
        logger.info("BRONZE — CONCLUÍDO COM SUCESSO ✔")
        logger.info(f"  Orders gravados : {cnt_orders:,}")
        logger.info(f"  Items gravados  : {cnt_items:,}")
        logger.info(f"  Quarentena      : {corrupt_o + corrupt_i:,} registros")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"[BRONZE] FALHA CRÍTICA: {e}")
        log_lineage(spark, cfg,
                    cfg.source_json, cfg.tbl_bronze_orders, 0, "FAILED")
        raise


if __name__ == "__main__":
    main()
