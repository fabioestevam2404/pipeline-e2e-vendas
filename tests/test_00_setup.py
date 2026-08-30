import pytest
from conftest import load_module

setup_mod = load_module("config/00_setup.py", "pipeline_e2e_setup")


def test_is_databricks_false_outside_databricks(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    assert setup_mod._is_databricks() is False


def test_is_databricks_true_when_runtime_env_set(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "15.4")
    assert setup_mod._is_databricks() is True


def test_is_mounted_false_without_dbutils():
    # dbutils não existe fora do Databricks — _is_mounted engole a exceção e
    # retorna False, nunca deveria propagar um NameError pra quem chama.
    assert setup_mod._is_mounted("/mnt/qualquer") is False
