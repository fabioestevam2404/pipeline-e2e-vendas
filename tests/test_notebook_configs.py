"""
Cada notebook (01_bronze.py, 02_silver.py, 03_gold.py, 04_optimize.py) define
sua própria PipelineConfig de fallback, usada sempre que `from config import
PipelineConfig` falha — e falha sempre fora do Databricks, já que config/ não
tem __init__.py (não é um pacote Python importável de verdade, só uma pasta
com convenção de nomes para %run). Ou seja: esse é o caminho que roda de
verdade em qualquer execução local/CI — validar aqui é o que importa.
"""
import pytest
from conftest import load_module

NOTEBOOKS = [
    ("notebooks/01_bronze.py", "pipeline_e2e_bronze"),
    ("notebooks/02_silver.py", "pipeline_e2e_silver_cfg"),
    ("notebooks/03_gold.py", "pipeline_e2e_gold"),
    ("notebooks/04_optimize.py", "pipeline_e2e_optimize"),
]


@pytest.fixture(params=NOTEBOOKS, ids=[n for n, _ in NOTEBOOKS])
def notebook_module(request):
    relative_path, module_name = request.param
    return load_module(relative_path, module_name)


def test_fallback_config_used_outside_databricks(notebook_module):
    # Confirma a premissa: `from config import PipelineConfig` falhou e o
    # notebook está mesmo usando a classe de fallback definida nele.
    assert notebook_module.PipelineConfig.__module__ == notebook_module.__name__


def test_fallback_config_accepts_valid_batch_date(notebook_module):
    cfg = notebook_module.PipelineConfig(batch_date="2026-02-20")
    assert cfg.batch_date == "2026-02-20"


def test_fallback_config_rejects_invalid_batch_date(notebook_module):
    with pytest.raises(ValueError):
        notebook_module.PipelineConfig(batch_date="'; DROP TABLE orders; --")


def test_is_databricks_false_locally(notebook_module):
    assert notebook_module._is_databricks() is False
