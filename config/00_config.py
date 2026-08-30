# =============================================================================
# PIPELINE E2E DE VENDAS — CONFIGURAÇÃO CENTRALIZADA
# =============================================================================
# Arquivo  : 00_config.py
# Propósito: Único ponto de verdade para todos os parâmetros do pipeline.
#            Importado por todos os outros notebooks/scripts.
# Uso      : from config import PipelineConfig, get_config
# =============================================================================

import os
from datetime import datetime
from dataclasses import dataclass, field

# Carrega variáveis do .env local (desenvolvimento local).
# override=False garante que variáveis já definidas no sistema têm prioridade.
# No Databricks, python-dotenv não está instalado — o except é silencioso.
try:
    from dotenv import load_dotenv
    load_dotenv(override=False)
except ImportError:
    pass

# -----------------------------------------------------------------------------
# DETECÇÃO DE AMBIENTE
# No Databricks os parâmetros chegam via dbutils.widgets (variável injetada
# no escopo do notebook, não um módulo importável).
# Fora do Databricks (testes locais), usamos variáveis de ambiente ou defaults.
# -----------------------------------------------------------------------------

def _get_param(key: str, default: str) -> str:
    """Lê parâmetro de widget Databricks, variável de ambiente ou default."""
    # 1) Tenta widget Databricks (dbutils é injetado no escopo, não importado)
    try:
        return dbutils.widgets.get(key)  # type: ignore[name-defined]
    except Exception:
        pass
    # 2) Tenta variável de ambiente
    val = os.environ.get(key.upper())
    if val:
        return val
    # 3) Usa default
    return default


# -----------------------------------------------------------------------------
# DATACLASS DE CONFIGURAÇÃO
# Centraliza todos os parâmetros com valores padrão para dev.
# Em produção, os valores são injetados pelo Databricks Workflow.
# -----------------------------------------------------------------------------

@dataclass
class PipelineConfig:
    # ── Ambiente ──────────────────────────────────────────────────────────────
    env:            str = "dev"
    # field(default_factory=...) garante avaliação no momento da instância,
    # não no momento do import do módulo.
    batch_date:     str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))

    # ── Catálogo Unity Catalog ────────────────────────────────────────────────
    catalog:        str = "pipeline_vendas"

    # ── Caminhos cloud (ABFSS para Azure ADLS Gen2 / s3a para AWS S3) ────────
    # Azure  : abfss://<container>@<storage_account>.dfs.core.windows.net/<path>
    # AWS    : s3a://<bucket>/<path>
    # Databricks Volumes (recomendado com Unity Catalog):
    #          /Volumes/<catalog>/<schema>/<volume>/<path>
    raw_path:       str = "/mnt/raw/vendas/"
    bronze_path:    str = "/mnt/bronze/vendas/"
    silver_path:    str = "/mnt/silver/vendas/"
    gold_path:      str = "/mnt/gold/vendas/"
    checkpoint_path:str = "/mnt/checkpoints/vendas/"
    audit_path:     str = "/mnt/audit/pipeline_vendas/"
    quarantine_path:str = "/mnt/bronze/vendas/_quarantine/"
    rejected_path:  str = "/mnt/silver/vendas/_rejected/"
    metrics_path:   str = "/mnt/monitoring/delta_metrics/"

    # ── Qualidade de dados ────────────────────────────────────────────────────
    min_pass_rate:      float = 0.90   # Mínimo 90% de registros aprovados
    min_price:          float = 0.01   # Preço unitário mínimo
    max_quantity:       int   = 10_000 # Quantidade máxima por item
    max_discount:       float = 1.0    # Desconto máximo (100%)
    max_null_pct:       float = 0.10   # Máximo de nulos permitido por coluna

    # ── Delta Lake / Otimização ───────────────────────────────────────────────
    vacuum_retention_hours: int = 168  # 7 dias de time travel
    target_file_size_mb:    int = 128  # Tamanho alvo de arquivo após OPTIMIZE
    shuffle_partitions:     int = 200  # spark.sql.shuffle.partitions

    # ── RBAC — Grupos Unity Catalog ───────────────────────────────────────────
    group_engineers:  str = "data_engineers"
    group_analysts:   str = "data_analysts"
    group_executives: str = "executives"
    group_platform:   str = "data_platform"

    def __post_init__(self) -> None:
        # batch_date chega via widget Databricks/env var (_get_param) e é
        # interpolado direto em SQL (spark.sql(f"...{cfg.batch_date}...")) em
        # vários notebooks — validar o formato aqui, uma única vez, fecha o
        # vetor de injeção para todos eles (ver bandit B608).
        datetime.strptime(self.batch_date, "%Y-%m-%d")

    # ── Schemas (bancos) ──────────────────────────────────────────────────────
    @property
    def bronze_db(self) -> str:
        return f"{self.catalog}.bronze"

    @property
    def silver_db(self) -> str:
        return f"{self.catalog}.silver"

    @property
    def gold_db(self) -> str:
        return f"{self.catalog}.gold"

    # ── Nomes de tabelas ──────────────────────────────────────────────────────
    # Bronze
    @property
    def tbl_bronze_orders(self) -> str:
        return f"{self.bronze_db}.orders_raw"

    @property
    def tbl_bronze_items(self) -> str:
        return f"{self.bronze_db}.order_items_raw"

    # Silver
    @property
    def tbl_silver_orders(self) -> str:
        return f"{self.silver_db}.orders"

    @property
    def tbl_silver_items(self) -> str:
        return f"{self.silver_db}.order_items"

    # Gold
    @property
    def tbl_gold_daily(self) -> str:
        return f"{self.gold_db}.sales_daily"

    @property
    def tbl_gold_weekly(self) -> str:
        return f"{self.gold_db}.sales_weekly"

    @property
    def tbl_gold_monthly(self) -> str:
        return f"{self.gold_db}.sales_monthly"

    @property
    def tbl_gold_category(self) -> str:
        return f"{self.gold_db}.sales_by_category"

    @property
    def tbl_gold_kpi(self) -> str:
        return f"{self.gold_db}.kpi_summary"

    # ── Fontes de dados ───────────────────────────────────────────────────────
    @property
    def source_json(self) -> str:
        return f"{self.raw_path}json/"

    @property
    def source_csv(self) -> str:
        return f"{self.raw_path}csv/"


def get_config() -> PipelineConfig:
    """
    Constrói PipelineConfig lendo parâmetros do ambiente (Databricks widgets,
    variáveis de ambiente ou defaults).

    Os defaults são lidos da própria instância padrão de PipelineConfig,
    mantendo um único ponto de verdade para os valores.
    """
    _d = PipelineConfig()  # instância com todos os defaults — fonte única de verdade
    return PipelineConfig(
        # ── Ambiente
        env=_get_param("env", _d.env),
        batch_date=_get_param("batch_date", _d.batch_date),
        # ── Catálogo
        catalog=_get_param("catalog", _d.catalog),
        # ── Caminhos
        raw_path=_get_param("raw_path", _d.raw_path),
        bronze_path=_get_param("bronze_path", _d.bronze_path),
        silver_path=_get_param("silver_path", _d.silver_path),
        gold_path=_get_param("gold_path", _d.gold_path),
        checkpoint_path=_get_param("checkpoint_path", _d.checkpoint_path),
        audit_path=_get_param("audit_path", _d.audit_path),
        quarantine_path=_get_param("quarantine_path", _d.quarantine_path),
        rejected_path=_get_param("rejected_path", _d.rejected_path),
        metrics_path=_get_param("metrics_path", _d.metrics_path),
        # ── Qualidade de dados (convertidos do tipo string do widget/env)
        min_pass_rate=float(_get_param("min_pass_rate", str(_d.min_pass_rate))),
        min_price=float(_get_param("min_price", str(_d.min_price))),
        max_quantity=int(_get_param("max_quantity", str(_d.max_quantity))),
        max_discount=float(_get_param("max_discount", str(_d.max_discount))),
        max_null_pct=float(_get_param("max_null_pct", str(_d.max_null_pct))),
        # ── Delta Lake / Otimização
        vacuum_retention_hours=int(_get_param("vacuum_retention_hours", str(_d.vacuum_retention_hours))),
        target_file_size_mb=int(_get_param("target_file_size_mb", str(_d.target_file_size_mb))),
        shuffle_partitions=int(_get_param("shuffle_partitions", str(_d.shuffle_partitions))),
        # ── RBAC
        group_engineers=_get_param("group_engineers", _d.group_engineers),
        group_analysts=_get_param("group_analysts", _d.group_analysts),
        group_executives=_get_param("group_executives", _d.group_executives),
        group_platform=_get_param("group_platform", _d.group_platform),
    )


# Instância global para uso direto nos notebooks
CFG = get_config()
