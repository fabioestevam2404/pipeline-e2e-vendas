from conftest import load_module

optimize_mod = load_module("notebooks/04_optimize.py", "pipeline_e2e_optimize_tables")


def _cfg():
    return optimize_mod.PipelineConfig(
        catalog="cat_teste",
        bronze_path="/mnt/bronze/",
        silver_path="/mnt/silver/",
        gold_path="/mnt/gold/",
    )


def test_returns_all_nine_managed_tables():
    tables = optimize_mod.get_tables_config(_cfg())
    assert len(tables) == 9
    assert set(tables) == {
        "cat_teste.bronze.orders_raw",
        "cat_teste.bronze.order_items_raw",
        "cat_teste.silver.orders",
        "cat_teste.silver.order_items",
        "cat_teste.gold.sales_daily",
        "cat_teste.gold.sales_weekly",
        "cat_teste.gold.sales_monthly",
        "cat_teste.gold.sales_by_category",
        "cat_teste.gold.kpi_summary",
    }


def test_bronze_tables_have_no_z_order_and_skip_analyze():
    tables = optimize_mod.get_tables_config(_cfg())
    for name in ("cat_teste.bronze.orders_raw", "cat_teste.bronze.order_items_raw"):
        assert tables[name]["z_order"] == []
        assert tables[name]["analyze"] is False
        assert tables[name]["vacuum"] is True


def test_silver_and_gold_tables_have_z_order_and_analyze():
    tables = optimize_mod.get_tables_config(_cfg())
    for name in (
        "cat_teste.silver.orders",
        "cat_teste.silver.order_items",
        "cat_teste.gold.sales_daily",
        "cat_teste.gold.kpi_summary",
    ):
        assert len(tables[name]["z_order"]) > 0
        assert tables[name]["analyze"] is True


def test_paths_derive_from_cfg_layer_paths():
    tables = optimize_mod.get_tables_config(_cfg())
    assert tables["cat_teste.bronze.orders_raw"]["path"] == "/mnt/bronze/orders/"
    assert tables["cat_teste.silver.orders"]["path"] == "/mnt/silver/orders/"
    assert tables["cat_teste.gold.sales_daily"]["path"] == "/mnt/gold/sales_daily/"


def test_every_table_has_a_description():
    tables = optimize_mod.get_tables_config(_cfg())
    assert all(t["description"] for t in tables.values())
