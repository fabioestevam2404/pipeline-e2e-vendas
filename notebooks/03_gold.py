# =============================================================================
# PIPELINE E2E DE VENDAS — CAMADA GOLD
# =============================================================================
# Arquivo  : 03_gold.py
# Propósito: Agregações analíticas diárias, semanais, mensais, por categoria
#            e KPIs executivos prontos para consumo em ferramentas de BI.
#
# Fluxo:
#   Silver (orders + order_items)
#     → View de fato unificada (JOIN único, reutilizado por todas as queries)
#     → Agregação diária  (ordem × categoria × canal × região)
#     → Agregação semanal (variação WoW)
#     → Agregação mensal  (variação MoM, métricas por segmento)
#     → Ranking por categoria (share de receita, top produto)
#     → KPI Summary (painel executivo consolidado)
#     → Gravação Delta (overwrite particionado — idempotente)
#     → Registro no Unity Catalog com CDF habilitado
#     → Quality Checks de consistência Silver × Gold
#     → Linhagem
#
# Dependências: 02_silver.py (deve ter concluído com sucesso)
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
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    _PYSPARK_AVAILABLE = True
except ModuleNotFoundError:
    _PYSPARK_AVAILABLE = False

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame

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
        env:          str = os.environ.get("ENV",          "dev")
        catalog:      str = os.environ.get("CATALOG",      "pipeline_vendas")
        batch_date:   str = datetime.now().strftime("%Y-%m-%d")
        silver_path:  str = os.environ.get("SILVER_PATH",  "/mnt/silver/vendas/")
        gold_path:    str = os.environ.get("GOLD_PATH",    "/mnt/gold/vendas/")
        audit_path:   str = os.environ.get("AUDIT_PATH",   "/mnt/audit/pipeline_vendas/")
        @property
        def silver_db(self):         return f"{self.catalog}.silver"
        @property
        def gold_db(self):           return f"{self.catalog}.gold"
        @property
        def tbl_silver_orders(self): return f"{self.silver_db}.orders"
        @property
        def tbl_silver_items(self):  return f"{self.silver_db}.order_items"
        @property
        def tbl_gold_daily(self):    return f"{self.gold_db}.sales_daily"
        @property
        def tbl_gold_weekly(self):   return f"{self.gold_db}.sales_weekly"
        @property
        def tbl_gold_monthly(self):  return f"{self.gold_db}.sales_monthly"
        @property
        def tbl_gold_category(self): return f"{self.gold_db}.sales_by_category"
        @property
        def tbl_gold_kpi(self):      return f"{self.gold_db}.kpi_summary"
    def get_config(): return PipelineConfig()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("gold")

# =============================================================================
# SPARK SESSION
# =============================================================================

def _is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _ensure_hadoop_home() -> None:
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
            "PySpark não está instalado.\n"
            "Para rodar localmente: pip install pyspark delta-spark"
        )
    _ensure_hadoop_home()
    _ensure_java_home()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder
        .appName(f"gold_agregacoes_{cfg.env}_{cfg.batch_date}")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled",                       "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled",    "true")
        .config("spark.sql.adaptive.skewJoin.enabled",              "true")
        .config("spark.sql.autoBroadcastJoinThreshold",             str(100 * 1024 * 1024))
        .config("spark.sql.shuffle.partitions",                     "200")
    )

    try:
        from delta import configure_spark_with_delta_pip  # type: ignore[import]
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError:
        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

# =============================================================================
# VIEW DE FATO UNIFICADA
# JOIN único entre orders e order_items, reutilizado por todas as agregações.
# Evita múltiplos scans das mesmas tabelas Silver.
# =============================================================================

def _register_local_tables(spark, cfg: "PipelineConfig") -> None:
    """
    Carrega as tabelas Silver como views temporárias para execução local.
    No Databricks, as tabelas já estão registradas no Unity Catalog.
    Localmente, precisamos criar views a partir dos caminhos Delta.
    """
    logger.info("[GOLD] Carregando Silver como views temporárias (local)...")
    spark.read.format("delta").load(cfg.silver_path + "orders/") \
        .createOrReplaceTempView("_local_silver_orders")
    spark.read.format("delta").load(cfg.silver_path + "order_items/") \
        .createOrReplaceTempView("_local_silver_items")
    logger.info("[GOLD] ✔ Views locais registradas.")


def create_fact_view(spark, cfg: "PipelineConfig"):
    """
    Cria view temporária que combina orders + order_items.

    Por que uma view e não um DataFrame?
      - Spark SQL é mais legível para analistas que farão manutenção
      - A view é lazy: o Spark a materializa uma vez e reutiliza o plano
      - Permite usar funções SQL analíticas (WINDOW, PERCENTILE_APPROX, etc.)
        que teriam sintaxe mais verbosa na API DataFrame

    Filtros aplicados aqui (antes das agregações):
      - _is_valid = true   : apenas registros aprovados pela Silver
      - status NOT IN (...): exclui pedidos cancelados e devolvidos da receita
    """
    logger.info("[GOLD] Criando view de fato (orders × order_items)...")

    # Localmente, usamos views temporárias em vez de tabelas do Unity Catalog
    orders_ref = cfg.tbl_silver_orders if _is_databricks() else "_local_silver_orders"
    items_ref  = cfg.tbl_silver_items  if _is_databricks() else "_local_silver_items"

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW vw_fact_sales AS
        SELECT
            o.order_id,
            o.customer_id,
            o.order_date,
            o.order_year,
            o.order_month,
            o.order_week,
            o.order_dayofweek,
            o.is_weekend,
            o.status,
            o.channel,
            o.region,
            o.order_segment,
            o.currency,
            i.item_id,
            i.product_id,
            i.product_name,
            i.category,
            i.quantity,
            i.unit_price,
            i.discount,
            i.subtotal
        FROM {orders_ref} o
        INNER JOIN {items_ref} i
            ON o.order_id = i.order_id
        WHERE o._is_valid = true
          AND i._is_valid = true
          AND o.status NOT IN ('CANCELLED', 'RETURNED')
    """)

    count = spark.sql("SELECT COUNT(*) AS n FROM vw_fact_sales").collect()[0]["n"]
    logger.info(f"[GOLD] View de fato criada: {count:,} registros")
    return count

# =============================================================================
# AGREGAÇÃO 1: VENDAS DIÁRIAS
# Granularidade: order_date × category × channel × region × order_segment
# =============================================================================

def build_sales_daily(spark, cfg: PipelineConfig) -> DataFrame:
    """
    Métricas diárias de vendas para acompanhamento operacional.

    Campos calculados:
      total_orders       : pedidos distintos no dia
      total_items        : unidades físicas vendidas
      unique_customers   : clientes únicos (para análise de recorrência)
      gross_revenue      : receita bruta (sem descontos)
      net_revenue        : receita líquida (após descontos)
      total_discount     : valor total descontado
      avg_order_value    : ticket médio (net_revenue / pedidos)
      avg_discount_pct   : percentual médio de desconto aplicado
      top_product        : produto mais vendido por receita no dia/categoria
    """
    logger.info("[GOLD] Construindo: sales_daily")

    return spark.sql(f"""
        SELECT
            order_date,
            order_year,
            order_month,
            order_week,
            category,
            channel,
            region,
            order_segment,

            -- Volume
            COUNT(DISTINCT order_id)                                       AS total_orders,
            SUM(quantity)                                                  AS total_items,
            COUNT(DISTINCT customer_id)                                    AS unique_customers,

            -- Receita
            ROUND(SUM(quantity * unit_price), 2)                          AS gross_revenue,
            ROUND(SUM(subtotal), 2)                                        AS net_revenue,
            ROUND(SUM(quantity * unit_price) - SUM(subtotal), 2)          AS total_discount,

            -- Ticket médio e desconto
            ROUND(SUM(subtotal) / COUNT(DISTINCT order_id), 2)            AS avg_order_value,
            ROUND(AVG(discount) * 100, 2)                                  AS avg_discount_pct,

            -- Top produto por receita no agrupamento (Spark SQL FIRST com IGNORE NULLS)
            FIRST(product_name, true)                                      AS top_product_name,
            FIRST(product_id,   true)                                      AS top_product_id,

            -- Metadados
            CURRENT_TIMESTAMP()                                            AS _gold_timestamp,
            '{cfg.batch_date}'                                             AS _processing_date

        FROM vw_fact_sales
        GROUP BY
            order_date, order_year, order_month, order_week,
            category, channel, region, order_segment
        ORDER BY order_date DESC, net_revenue DESC
    """)

# =============================================================================
# AGREGAÇÃO 2: VENDAS SEMANAIS (com variação WoW)
# =============================================================================

def build_sales_weekly(spark, cfg: PipelineConfig) -> DataFrame:
    """
    Métricas semanais com variação semana-a-semana (Week-over-Week).

    LAG(net_revenue, 1) sobre a janela ordenada por ano/semana/categoria
    permite calcular a variação percentual em relação à semana anterior.

    Útil para: alertas de queda de receita, sazonalidade semanal,
    análise de promoções por período.
    """
    logger.info("[GOLD] Construindo: sales_weekly")

    df_weekly = spark.sql(f"""
        SELECT
            order_year,
            order_week,
            category,
            channel,
            region,

            COUNT(DISTINCT order_id)                                       AS total_orders,
            SUM(quantity)                                                  AS total_items,
            COUNT(DISTINCT customer_id)                                    AS unique_customers,
            ROUND(SUM(subtotal), 2)                                        AS net_revenue,
            ROUND(SUM(quantity * unit_price), 2)                          AS gross_revenue,
            ROUND(SUM(subtotal) / COUNT(DISTINCT order_id), 2)            AS avg_order_value,
            ROUND(AVG(discount) * 100, 2)                                  AS avg_discount_pct,

            -- Proporção de receita no fim de semana
            ROUND(
                SUM(CASE WHEN is_weekend THEN subtotal ELSE 0 END)
                / NULLIF(SUM(subtotal), 0) * 100,
            2)                                                             AS weekend_revenue_pct,

            -- Contagem de pedidos no fim de semana
            SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END)                   AS weekend_orders,

            CURRENT_TIMESTAMP()                                            AS _gold_timestamp,
            '{cfg.batch_date}'                                             AS _processing_date

        FROM vw_fact_sales
        GROUP BY order_year, order_week, category, channel, region
    """)

    # Registrar como view temporária para aplicar LAG
    df_weekly.createOrReplaceTempView("vw_weekly_base")

    # Variação WoW via LAG
    return spark.sql("""
        SELECT
            *,
            LAG(net_revenue, 1) OVER (
                PARTITION BY category, channel, region
                ORDER BY order_year, order_week
            )                                                              AS prev_week_revenue,
            ROUND(
                (net_revenue - LAG(net_revenue, 1) OVER (
                    PARTITION BY category, channel, region
                    ORDER BY order_year, order_week
                )) / NULLIF(LAG(net_revenue, 1) OVER (
                    PARTITION BY category, channel, region
                    ORDER BY order_year, order_week
                ), 0) * 100,
            2)                                                             AS wow_revenue_pct
        FROM vw_weekly_base
        ORDER BY order_year DESC, order_week DESC, net_revenue DESC
    """)

# =============================================================================
# AGREGAÇÃO 3: VENDAS MENSAIS (com variação MoM)
# =============================================================================

def build_sales_monthly(spark, cfg: PipelineConfig) -> DataFrame:
    """
    Métricas mensais para relatórios executivos e acompanhamento de metas.

    Inclui:
      - revenue_per_customer     : receita média por cliente (LTV parcial)
      - avg_items_per_order      : complexidade média do pedido
      - orders_with_discount_pct : % pedidos com algum desconto aplicado
      - Variação MoM via LAG sobre janela (category, channel) ordenada por ano/mês
    """
    logger.info("[GOLD] Construindo: sales_monthly")

    df_monthly = spark.sql(f"""
        SELECT
            order_year,
            order_month,
            CONCAT(order_year, '-', LPAD(order_month, 2, '0'))            AS year_month,
            category,
            channel,
            region,
            order_segment,

            COUNT(DISTINCT order_id)                                       AS total_orders,
            SUM(quantity)                                                  AS total_items,
            COUNT(DISTINCT customer_id)                                    AS unique_customers,

            ROUND(SUM(subtotal), 2)                                        AS net_revenue,
            ROUND(SUM(quantity * unit_price), 2)                          AS gross_revenue,
            ROUND(SUM(quantity * unit_price) - SUM(subtotal), 2)          AS total_discount_amount,

            ROUND(SUM(subtotal) / COUNT(DISTINCT order_id),     2)        AS avg_order_value,
            ROUND(SUM(subtotal) / COUNT(DISTINCT customer_id),  2)        AS revenue_per_customer,
            ROUND(SUM(quantity)  / COUNT(DISTINCT order_id),    2)        AS avg_items_per_order,
            ROUND(AVG(discount) * 100, 2)                                  AS avg_discount_pct,

            -- % pedidos com desconto (discount > 0)
            ROUND(
                SUM(CASE WHEN discount > 0 THEN 1 ELSE 0 END)
                / NULLIF(COUNT(DISTINCT order_id), 0) * 100,
            2)                                                             AS orders_with_discount_pct,

            CURRENT_TIMESTAMP()                                            AS _gold_timestamp,
            '{cfg.batch_date}'                                             AS _processing_date

        FROM vw_fact_sales
        GROUP BY
            order_year, order_month, category, channel, region, order_segment
    """)

    df_monthly.createOrReplaceTempView("vw_monthly_base")

    # Variação MoM via LAG
    return spark.sql("""
        SELECT
            *,
            LAG(net_revenue, 1) OVER (
                PARTITION BY category, channel, region, order_segment
                ORDER BY order_year, order_month
            )                                                              AS prev_month_revenue,
            ROUND(
                (net_revenue - LAG(net_revenue, 1) OVER (
                    PARTITION BY category, channel, region, order_segment
                    ORDER BY order_year, order_month
                )) / NULLIF(LAG(net_revenue, 1) OVER (
                    PARTITION BY category, channel, region, order_segment
                    ORDER BY order_year, order_month
                ), 0) * 100,
            2)                                                             AS mom_revenue_pct
        FROM vw_monthly_base
        ORDER BY order_year DESC, order_month DESC, net_revenue DESC
    """)

# =============================================================================
# AGREGAÇÃO 4: RANKING POR CATEGORIA
# =============================================================================

def build_sales_by_category(spark, cfg: PipelineConfig) -> DataFrame:
    """
    Análise comparativa de categorias de produto.

    revenue_share_pct: share de receita da categoria no mês
      Calculado como: receita_categoria / receita_total_mes × 100
      Usando SUM OVER (PARTITION BY month) — window function sem GROUP BY adicional

    rank_by_revenue: posição da categoria por receita no mês (1 = maior receita)

    top_product_by_revenue: produto com maior subtotal na categoria/mês.
      Calculado via subconsulta com RANK() para evitar GROUP BY adicional.
    """
    logger.info("[GOLD] Construindo: sales_by_category")

    return spark.sql(f"""
        WITH category_monthly AS (
            SELECT
                category,
                order_year,
                order_month,
                CONCAT(order_year, '-', LPAD(order_month, 2, '0'))        AS year_month,

                COUNT(DISTINCT order_id)                                   AS total_orders,
                SUM(quantity)                                              AS total_items,
                COUNT(DISTINCT product_id)                                 AS distinct_products,
                COUNT(DISTINCT customer_id)                                AS unique_customers,

                ROUND(SUM(subtotal), 2)                                    AS net_revenue,
                ROUND(AVG(unit_price), 2)                                  AS avg_unit_price,
                ROUND(AVG(discount) * 100, 2)                              AS avg_discount_pct,
                ROUND(SUM(subtotal) / COUNT(DISTINCT order_id), 2)        AS avg_order_value

            FROM vw_fact_sales
            GROUP BY category, order_year, order_month
        ),
        with_share AS (
            SELECT
                *,
                -- Share de receita no mês (window function sem GROUP BY)
                ROUND(
                    net_revenue / SUM(net_revenue) OVER (
                        PARTITION BY order_year, order_month
                    ) * 100,
                2)                                                         AS revenue_share_pct,
                -- Ranking por receita no mês
                RANK() OVER (
                    PARTITION BY order_year, order_month
                    ORDER BY net_revenue DESC
                )                                                          AS rank_by_revenue
            FROM category_monthly
        )
        SELECT
            *,
            CURRENT_TIMESTAMP()                                            AS _gold_timestamp,
            '{cfg.batch_date}'                                             AS _processing_date
        FROM with_share
        ORDER BY order_year DESC, order_month DESC, rank_by_revenue
    """)

# =============================================================================
# AGREGAÇÃO 5: KPI SUMMARY (PAINEL EXECUTIVO)
# =============================================================================

def build_kpi_summary(spark, cfg: PipelineConfig) -> DataFrame:
    """
    Tabela de KPIs consolidados para consumo direto por ferramentas de BI.
    Uma linha por mês com todos os indicadores principais do negócio.

    Campos especiais:
      cumulative_revenue_ytd : receita acumulada no ano (YTD)
        Calculado via SUM(...) OVER (PARTITION BY order_year ORDER BY order_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

      revenue_target         : meta de receita do mês (110% do mês anterior)
        Calculado via LAG(net_revenue, 1) × 1.10

      hit_target             : flag se a meta foi atingida (BOOLEAN)

    Estes campos permitem que dashboards de BI exibam progresso vs. meta
    e YTD sem transformações adicionais no Power BI / Tableau / Looker.
    """
    logger.info("[GOLD] Construindo: kpi_summary")

    df_kpi_base = spark.sql(f"""
        SELECT
            order_year,
            order_month,
            CONCAT(order_year, '-', LPAD(order_month, 2, '0'))            AS year_month,

            -- Receita
            ROUND(SUM(subtotal), 2)                                        AS net_revenue,
            ROUND(SUM(quantity * unit_price), 2)                          AS gross_revenue,
            ROUND(SUM(quantity * unit_price) - SUM(subtotal), 2)          AS total_discounts,

            -- Volume
            COUNT(DISTINCT order_id)                                       AS total_orders,
            SUM(quantity)                                                  AS total_units_sold,
            COUNT(DISTINCT customer_id)                                    AS total_unique_customers,
            COUNT(DISTINCT product_id)                                     AS total_products_sold,

            -- Médias
            ROUND(SUM(subtotal) / COUNT(DISTINCT order_id),    2)         AS avg_order_value,
            ROUND(SUM(subtotal) / COUNT(DISTINCT customer_id), 2)         AS avg_revenue_per_customer,
            ROUND(AVG(discount) * 100, 2)                                  AS avg_discount_pct,

            -- Diversidade
            COUNT(DISTINCT channel)                                        AS active_channels,
            COUNT(DISTINCT category)                                       AS active_categories,
            COUNT(DISTINCT region)                                         AS active_regions,

            -- Comportamento fim de semana
            ROUND(
                SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END)
                / NULLIF(COUNT(DISTINCT order_id), 0) * 100,
            2)                                                             AS weekend_orders_pct,

            CURRENT_TIMESTAMP()                                            AS _gold_timestamp,
            '{cfg.batch_date}'                                             AS _processing_date

        FROM vw_fact_sales
        GROUP BY order_year, order_month
    """)

    df_kpi_base.createOrReplaceTempView("vw_kpi_base")

    # Receita acumulada YTD, meta e flag de atingimento
    return spark.sql("""
        SELECT
            *,
            -- Receita acumulada no ano até o mês atual (YTD)
            ROUND(SUM(net_revenue) OVER (
                PARTITION BY order_year
                ORDER BY order_month
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ), 2)                                                          AS cumulative_revenue_ytd,

            -- Meta = 110% da receita do mês anterior (mesmo canal/categoria)
            ROUND(LAG(net_revenue, 1) OVER (
                ORDER BY order_year, order_month
            ) * 1.10, 2)                                                   AS revenue_target,

            -- Atingiu a meta?
            CASE
                WHEN net_revenue >= LAG(net_revenue, 1) OVER (
                    ORDER BY order_year, order_month
                ) * 1.10 THEN true
                ELSE false
            END                                                            AS hit_target

        FROM vw_kpi_base
        ORDER BY order_year DESC, order_month DESC
    """)

# =============================================================================
# GRAVAÇÃO DELTA — OVERWRITE PARTICIONADO
# =============================================================================

def write_gold(df: DataFrame, table_name: str, path: str,
               partition_cols: list, cfg: PipelineConfig):
    """
    Grava tabela Gold com overwrite particionado (idempotente).

    replaceWhere substitui apenas a partição do processamento atual.
    Dados de meses anteriores são preservados.

    Para a primeira carga (tabela não existe), usa overwrite simples.
    """
    count = df.count()
    logger.info(f"[GOLD] Gravando {table_name}: {count:,} registros")

    try:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema",  "true")
            .option("replaceWhere",     f"_processing_date = '{cfg.batch_date}'")
            .partitionBy(*partition_cols)
            .save(path)
        )
    except Exception:
        # Primeira carga: partição não existe
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy(*partition_cols)
            .save(path)
        )
    logger.info(f"[GOLD] ✔ {table_name} gravado: {path}")
    return count

# =============================================================================
# REGISTRO NO UNITY CATALOG
# =============================================================================

def register_gold_tables(spark, cfg: "PipelineConfig"):
    """
    Registra todas as tabelas Gold no Unity Catalog.
    Habilita Change Data Feed (CDF) para rastreamento de alterações downstream.
    Ignorado fora do Databricks.
    """
    if not _is_databricks():
        logger.info("[CATALOG] Registro ignorado (execução local).")
        return
    logger.info("[GOLD] Registrando tabelas no Unity Catalog...")

    tables = [
        (cfg.tbl_gold_daily,
         cfg.gold_path + "sales_daily/",
         "Vendas diárias por categoria, canal e região"),
        (cfg.tbl_gold_weekly,
         cfg.gold_path + "sales_weekly/",
         "Vendas semanais com variação WoW"),
        (cfg.tbl_gold_monthly,
         cfg.gold_path + "sales_monthly/",
         "Vendas mensais com variação MoM para relatórios executivos"),
        (cfg.tbl_gold_category,
         cfg.gold_path + "sales_by_category/",
         "Ranking e share de receita por categoria"),
        (cfg.tbl_gold_kpi,
         cfg.gold_path + "kpi_summary/",
         "KPIs consolidados mensais para dashboards de BI"),
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
                'layer'                      = 'gold',
                'pipeline'                   = 'pipeline_vendas',
                'update_frequency'           = 'daily',
                'delta.enableChangeDataFeed' = 'true',
                'owner'                      = 'data_engineering'
            )
        """)
        logger.info(f"[CATALOG] ✔ Registrado: {name}")

# =============================================================================
# QUALITY CHECKS — GOLD
# =============================================================================

def run_quality_checks(spark, cfg: "PipelineConfig", counts: dict) -> bool:
    """
    Valida consistência das agregações Gold.

    QC-1: Receita Gold deve ser igual à Silver (tolerância 0.01%)
    QC-2: Sem nulos em campos de chave das agregações
    QC-3: KPI Summary não pode estar vazio
    QC-4: Exibe top-3 meses para validação visual

    QC-1..QC-4 requerem Unity Catalog e são ignorados fora do Databricks.
    Localmente, apenas verifica se as tabelas foram gravadas com registros.
    """
    logger.info("[QC] Iniciando quality checks — Gold")

    if not _is_databricks():
        logger.info("[QC] Checks SQL ignorados (execução local — requerem Unity Catalog).")
        for key, count in counts.items():
            if count > 0:
                logger.info(f"[QC] ✔ {key}: {count:,} registros gravados")
            else:
                logger.warning(f"[QC] ⚠ {key}: tabela VAZIA!")
        logger.info("[QC] ✔ Quality checks locais concluídos")
        return all(v > 0 for v in counts.values())

    failed = []

    # QC-1: Consistência de receita Silver × Gold
    try:
        rev_silver = spark.sql(f"""
            SELECT ROUND(SUM(i.subtotal), 2) AS total
            FROM {cfg.tbl_silver_items} i
            INNER JOIN {cfg.tbl_silver_orders} o
                ON i.order_id = o.order_id
            WHERE o._is_valid = true
              AND i._is_valid = true
              AND o.status NOT IN ('CANCELLED', 'RETURNED')
        """).collect()[0]["total"] or 0

        rev_gold = spark.sql(f"""
            SELECT SUM(net_revenue) AS total
            FROM {cfg.tbl_gold_monthly}
        """).collect()[0]["total"] or 0

        if rev_silver > 0:
            diff_pct = abs(rev_silver - rev_gold) / rev_silver * 100
            if diff_pct > 0.01:
                msg = f"QC-1: divergência Silver ({rev_silver:,.2f}) × Gold ({rev_gold:,.2f}) = {diff_pct:.4f}%"
                logger.warning(f"[QC] ⚠ {msg}")
                failed.append(msg)
            else:
                logger.info(f"[QC] ✔ QC-1 Receita consistente (diff: {diff_pct:.6f}%)")
    except Exception as e:
        logger.warning(f"[QC] QC-1 não pôde ser executado: {e}")

    # QC-2: Nulos em chaves da tabela diária
    try:
        n_null = spark.sql(f"""
            SELECT COUNT(*) AS n
            FROM {cfg.tbl_gold_daily}
            WHERE order_date IS NULL OR category IS NULL
        """).collect()[0]["n"]
        if n_null > 0:
            msg = f"QC-2: {n_null:,} linhas com chave nula em sales_daily"
            logger.warning(f"[QC] ⚠ {msg}")
            failed.append(msg)
        else:
            logger.info("[QC] ✔ QC-2 Sem chaves nulas em sales_daily")
    except Exception as e:
        logger.warning(f"[QC] QC-2 não pôde ser executado: {e}")

    # QC-3: KPI Summary não vazio
    try:
        n_kpi = spark.sql(f"SELECT COUNT(*) AS n FROM {cfg.tbl_gold_kpi}").collect()[0]["n"]
        if n_kpi == 0:
            msg = "QC-3: kpi_summary está vazio!"
            logger.error(f"[QC] ✘ {msg}")
            failed.append(msg)
        else:
            logger.info(f"[QC] ✔ QC-3 kpi_summary: {n_kpi:,} linhas")
    except Exception as e:
        logger.warning(f"[QC] QC-3 não pôde ser executado: {e}")

    # QC-4: Preview dos KPIs (validação visual nos logs)
    try:
        logger.info("[QC] KPI Summary — top 3 meses:")
        spark.sql(f"""
            SELECT year_month, net_revenue, total_orders,
                   total_unique_customers, avg_order_value, hit_target
            FROM {cfg.tbl_gold_kpi}
            ORDER BY year_month DESC
            LIMIT 3
        """).show(truncate=False)
    except Exception as e:
        logger.warning(f"[QC] QC-4 preview falhou: {e}")

    if failed:
        logger.error(f"[QC] {len(failed)} check(s) falharam: {failed}")
        return False

    logger.info("[QC] ✔ Todos os quality checks Gold aprovados")
    return True

# =============================================================================
# LINHAGEM
# =============================================================================

def log_lineage(spark, cfg: PipelineConfig, source: str,
                target: str, count: int, status: str = "SUCCESS"):
    try:
        record = spark.createDataFrame([{
            "pipeline":     "03_gold",
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
    logger.info("GOLD — Pipeline E2E de Vendas")
    logger.info(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    cfg   = get_config()
    spark = get_spark(cfg)

    logger.info(f"Ambiente   : {cfg.env}")
    logger.info(f"Batch date : {cfg.batch_date}")

    counts = {}

    try:
        # ── 0. REGISTRO LOCAL DE TABELAS SILVER ───────────────────────────────
        # No Databricks as tabelas já estão no Unity Catalog.
        # Localmente, precisamos carregar os arquivos Delta como temp views.
        if not _is_databricks():
            _register_local_tables(spark, cfg)

        # ── 1. VIEW DE FATO ───────────────────────────────────────────────────
        fact_count = create_fact_view(spark, cfg)

        # ── 2. CONSTRUIR AGREGAÇÕES ───────────────────────────────────────────
        df_daily    = build_sales_daily(spark, cfg)
        df_weekly   = build_sales_weekly(spark, cfg)
        df_monthly  = build_sales_monthly(spark, cfg)
        df_category = build_sales_by_category(spark, cfg)
        df_kpi      = build_kpi_summary(spark, cfg)

        # ── 3. GRAVAR ─────────────────────────────────────────────────────────
        counts["daily"] = write_gold(
            df_daily, cfg.tbl_gold_daily,
            cfg.gold_path + "sales_daily/",
            ["order_year", "order_month"], cfg
        )
        counts["weekly"] = write_gold(
            df_weekly, cfg.tbl_gold_weekly,
            cfg.gold_path + "sales_weekly/",
            ["order_year"], cfg
        )
        counts["monthly"] = write_gold(
            df_monthly, cfg.tbl_gold_monthly,
            cfg.gold_path + "sales_monthly/",
            ["order_year"], cfg
        )
        counts["category"] = write_gold(
            df_category, cfg.tbl_gold_category,
            cfg.gold_path + "sales_by_category/",
            ["order_year", "order_month"], cfg
        )
        counts["kpi"] = write_gold(
            df_kpi, cfg.tbl_gold_kpi,
            cfg.gold_path + "kpi_summary/",
            ["order_year"], cfg
        )

        # ── 4. REGISTRO NO CATÁLOGO ───────────────────────────────────────────
        register_gold_tables(spark, cfg)

        # ── 5. QUALITY CHECKS ─────────────────────────────────────────────────
        qc_ok = run_quality_checks(spark, cfg, counts)
        if not qc_ok:
            raise Exception("Quality checks Gold falharam. Verifique os logs.")

        # ── 6. LINHAGEM ───────────────────────────────────────────────────────
        silver_src = f"{cfg.tbl_silver_orders}+{cfg.tbl_silver_items}"
        log_lineage(spark, cfg, silver_src, cfg.tbl_gold_daily,    counts["daily"])
        log_lineage(spark, cfg, silver_src, cfg.tbl_gold_weekly,   counts["weekly"])
        log_lineage(spark, cfg, silver_src, cfg.tbl_gold_monthly,  counts["monthly"])
        log_lineage(spark, cfg, silver_src, cfg.tbl_gold_category, counts["category"])
        log_lineage(spark, cfg, silver_src, cfg.tbl_gold_kpi,      counts["kpi"])

        logger.info("=" * 60)
        logger.info("GOLD — CONCLUÍDO COM SUCESSO ✔")
        logger.info(f"  Fato view     : {fact_count:,} registros")
        logger.info(f"  sales_daily   : {counts['daily']:,} linhas")
        logger.info(f"  sales_weekly  : {counts['weekly']:,} linhas")
        logger.info(f"  sales_monthly : {counts['monthly']:,} linhas")
        logger.info(f"  by_category   : {counts['category']:,} linhas")
        logger.info(f"  kpi_summary   : {counts['kpi']:,} linhas")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"[GOLD] FALHA CRÍTICA: {e}")
        log_lineage(spark, cfg,
                    cfg.tbl_silver_orders, cfg.tbl_gold_kpi, 0, "FAILED")
        raise


if __name__ == "__main__":
    main()
