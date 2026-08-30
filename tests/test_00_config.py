import pytest
from conftest import load_module

config_mod = load_module("config/00_config.py", "pipeline_e2e_config")


def test_default_config_has_todays_batch_date():
    from datetime import datetime
    cfg = config_mod.PipelineConfig()
    assert cfg.batch_date == datetime.now().strftime("%Y-%m-%d")


def test_post_init_accepts_valid_batch_date():
    cfg = config_mod.PipelineConfig(batch_date="2026-01-15")
    assert cfg.batch_date == "2026-01-15"


@pytest.mark.parametrize(
    "bad_date",
    [
        "2026-01-15'; DROP TABLE orders; --",
        "not-a-date",
        "2026/01/15",
        "",
    ],
)
def test_post_init_rejects_invalid_batch_date(bad_date):
    # Isso é a mitigação real do bandit B608: batch_date é interpolado direto
    # em spark.sql(f"...") em vários notebooks, então qualquer valor que não
    # seja uma data ISO bem formada tem que falhar aqui, antes de chegar
    # perto de uma query.
    with pytest.raises(ValueError):
        config_mod.PipelineConfig(batch_date=bad_date)


def test_table_name_properties_follow_catalog():
    cfg = config_mod.PipelineConfig(catalog="minha_empresa")
    assert cfg.bronze_db == "minha_empresa.bronze"
    assert cfg.silver_db == "minha_empresa.silver"
    assert cfg.gold_db == "minha_empresa.gold"
    assert cfg.tbl_bronze_orders == "minha_empresa.bronze.orders_raw"
    assert cfg.tbl_bronze_items == "minha_empresa.bronze.order_items_raw"
    assert cfg.tbl_silver_orders == "minha_empresa.silver.orders"
    assert cfg.tbl_silver_items == "minha_empresa.silver.order_items"
    assert cfg.tbl_gold_daily == "minha_empresa.gold.sales_daily"
    assert cfg.tbl_gold_weekly == "minha_empresa.gold.sales_weekly"
    assert cfg.tbl_gold_monthly == "minha_empresa.gold.sales_monthly"
    assert cfg.tbl_gold_category == "minha_empresa.gold.sales_by_category"
    assert cfg.tbl_gold_kpi == "minha_empresa.gold.kpi_summary"


def test_source_paths_derive_from_raw_path():
    cfg = config_mod.PipelineConfig(raw_path="/mnt/raw/vendas/")
    assert cfg.source_json == "/mnt/raw/vendas/json/"
    assert cfg.source_csv == "/mnt/raw/vendas/csv/"


def test_get_param_falls_back_to_env_var(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("MY_PARAM", "from-env")
    assert config_mod._get_param("my_param", "default-value") == "from-env"


def test_get_param_falls_back_to_default_when_unset(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("UNSET_PARAM", raising=False)
    assert config_mod._get_param("unset_param", "default-value") == "default-value"


def test_get_config_reads_env_overrides(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("CATALOG", "catalogo_de_teste")
    monkeypatch.setenv("BATCH_DATE", "2026-03-01")
    cfg = config_mod.get_config()
    assert cfg.catalog == "catalogo_de_teste"
    assert cfg.batch_date == "2026-03-01"


def test_get_config_rejects_invalid_batch_date_from_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("BATCH_DATE", "invalido")
    with pytest.raises(ValueError):
        config_mod.get_config()
