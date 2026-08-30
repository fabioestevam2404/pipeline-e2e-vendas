"""
tests/conftest.py
Os arquivos de config/notebooks têm nomes com prefixo numérico (01_bronze.py,
00_config.py...) — convenção deliberada do repositório para refletir a ordem
de execução no Databricks Workflow, mas isso os torna inválidos como nomes de
módulo Python (`import 01_bronze` é SyntaxError). load_module() contorna isso
carregando o arquivo pelo caminho via importlib, o mesmo mecanismo que
qualquer runner de notebook usaria.
"""
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parent.parent


def load_module(relative_path: str, module_name: str) -> ModuleType:
    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module
