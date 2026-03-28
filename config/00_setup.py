# =============================================================================
# PIPELINE E2E DE VENDAS — SETUP DE AMBIENTE
# =============================================================================
# Arquivo  : 00_setup.py
# Propósito: Preparar o ambiente Databricks antes de rodar o pipeline:
#              1) Criar mount points (ou validar caminhos ABFSS/S3)
#              2) Criar catálogos e schemas no Unity Catalog
#              3) Criar diretórios base no storage
#              4) Validar conectividade e permissões
# Execução : Primeira task do Workflow — todas as outras dependem desta.
# =============================================================================

import glob
import logging
import os
import platform
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

# PySpark é disponível apenas no Databricks / ambiente com PySpark instalado.
# O import lazy evita ModuleNotFoundError ao importar este módulo localmente.
try:
    from pyspark.sql import SparkSession
    _PYSPARK_AVAILABLE = True
except ModuleNotFoundError:
    _PYSPARK_AVAILABLE = False

if TYPE_CHECKING:
    from pyspark.sql import SparkSession  # apenas para type checkers (mypy, Pylance)

# Importar configuração centralizada
# No Databricks, certifique-se de que 00_config.py está no mesmo diretório
# ou acessível via %run ./00_config ou sys.path.append
try:
    from config import get_config, PipelineConfig
except ImportError:
    # Fallback: define config inline se import falhar
    from dataclasses import dataclass
    @dataclass
    class PipelineConfig:
        env:             str = "dev"
        batch_date:      str = datetime.now().strftime("%Y-%m-%d")
        catalog:         str = "pipeline_vendas"
        raw_path:        str = "/mnt/raw/vendas/"
        bronze_path:     str = "/mnt/bronze/vendas/"
        silver_path:     str = "/mnt/silver/vendas/"
        gold_path:       str = "/mnt/gold/vendas/"
        audit_path:      str = "/mnt/audit/pipeline_vendas/"
        metrics_path:    str = "/mnt/monitoring/delta_metrics/"
        quarantine_path: str = "/mnt/bronze/vendas/_quarantine/"
        rejected_path:   str = "/mnt/silver/vendas/_rejected/"
    def get_config(): return PipelineConfig()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("pipeline_setup")

# =============================================================================
# SPARK SESSION
# =============================================================================

def _ensure_hadoop_home() -> None:
    """
    Configura HADOOP_HOME com winutils.exe para execução do PySpark no Windows.
    O winutils.exe é necessário para operações de filesystem (permissões) do Hadoop.
    Baixa automaticamente os binários na primeira execução (~1 MB).
    Em sistemas não-Windows ou quando HADOOP_HOME já está definido, não faz nada.
    """
    if platform.system() != "Windows" or os.environ.get("HADOOP_HOME"):
        return

    hadoop_dir = Path.home() / ".hadoop-winutils"
    bin_dir = hadoop_dir / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)

    # winutils para Hadoop 3.3.5 (compatível com PySpark 3.5.x)
    base_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin"
    for fname in ("winutils.exe", "hadoop.dll"):
        dest = bin_dir / fname
        if not dest.exists():
            logger.info(f"[HADOOP] Baixando {fname} (necessário para PySpark no Windows)...")
            try:
                urllib.request.urlretrieve(f"{base_url}/{fname}", dest)
                logger.info(f"[HADOOP] {fname} baixado com sucesso.")
            except Exception as e:
                logger.warning(f"[HADOOP] Falha ao baixar {fname}: {e}")

    os.environ["HADOOP_HOME"] = str(hadoop_dir)
    os.environ["hadoop.home.dir"] = str(hadoop_dir)
    logger.info(f"[HADOOP] HADOOP_HOME configurado: {hadoop_dir}")


def _ensure_java_home() -> None:
    """
    Detecta e configura JAVA_HOME automaticamente se não estiver definido.
    Necessário ao rodar localmente no Windows quando o terminal foi aberto
    antes de o JDK ser instalado (setx só afeta sessões futuras).
    """
    if os.environ.get("JAVA_HOME"):
        return
    candidates = (
        glob.glob(r"C:\Program Files\Microsoft\jdk-*")
        + glob.glob(r"C:\Program Files\Eclipse Adoptium\jdk-*")
        + glob.glob(r"C:\Program Files\Java\jdk*")
    )
    for path in sorted(candidates, reverse=True):  # prefere versão mais recente
        if os.path.exists(os.path.join(path, "bin", "java.exe")):
            os.environ["JAVA_HOME"] = path
            logger.info(f"[JAVA] JAVA_HOME detectado automaticamente: {path}")
            return
    logger.warning("[JAVA] Java não encontrado. Instale o JDK e defina JAVA_HOME.")


def get_spark() -> "SparkSession":
    if not _PYSPARK_AVAILABLE:
        raise RuntimeError(
            "PySpark não está instalado neste ambiente.\n"
            "Para rodar localmente: pip install pyspark delta-spark\n"
            "Em produção, execute este script no Databricks."
        )
    _ensure_hadoop_home()
    _ensure_java_home()

    builder = (
        SparkSession.builder
        .appName("pipeline_setup")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled",                    "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    # Fora do Databricks, o delta-spark instalado via pip precisa de
    # configure_spark_with_delta_pip para injetar os JARs do Delta Lake.
    # No Databricks, o Delta já está no classpath do runtime — não é necessário.
    try:
        from delta import configure_spark_with_delta_pip  # type: ignore[import]
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        logger.info("[SPARK] Delta Lake configurado via configure_spark_with_delta_pip (modo local).")
    except ImportError:
        # Databricks ou ambiente com Delta já no classpath
        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

# =============================================================================
# MOUNT POINTS (apenas para workspaces sem Unity Catalog Volumes)
# Se estiver usando Unity Catalog Volumes (/Volumes/...), ignore esta seção
# =============================================================================

def _is_mounted(mount_point: str) -> bool:
    """Verifica se um mount point já existe."""
    try:
        return any(m.mountPoint == mount_point
                   for m in dbutils.fs.mounts())  # type: ignore[name-defined]
    except Exception:
        return False


def mount_storage(mount_point: str, source: str, extra_configs: dict = None):
    """
    Monta um path de cloud storage em /mnt/<name>.

    Para AWS S3 com IAM Role (Instance Profile):
        source = "s3a://meu-bucket/caminho"
        extra_configs = {}   # IAM Role cuida da autenticação

    Para Azure ADLS Gen2 com Service Principal:
        source = "abfss://container@storage.dfs.core.windows.net/"
        extra_configs = {
            "fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "...",
            "fs.azure.account.oauth2.client.id": "<client_id>",
            "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(...),
            "fs.azure.account.oauth2.client.endpoint": "...",
        }
    """
    try:
        if _is_mounted(mount_point):
            logger.info(f"[MOUNT] Já montado: {mount_point} → {source}")
            return

        dbutils.fs.mount(  # type: ignore[name-defined]
            source=source,
            mount_point=mount_point,
            extra_configs=extra_configs or {}
        )
        logger.info(f"[MOUNT] ✔ Montado: {mount_point} → {source}")

    except Exception as e:
        logger.warning(f"[MOUNT] Falha ao montar {mount_point}: {e}")
        logger.warning("[MOUNT] Continuando sem mount point — usando caminho direto.")


def setup_mounts(cfg: PipelineConfig):
    """
    Configura todos os mount points necessários para o pipeline.

    ⚠ ADAPTE os valores abaixo para o seu ambiente:
       - Para AWS: substitua 'meu-bucket-s3' pelo nome do seu bucket
       - Para Azure: substitua pelas credenciais do seu ADLS Gen2
       - Para GCP: substitua pelo nome do seu GCS bucket

    Alternativa moderna (Unity Catalog Volumes):
      Use /Volumes/<catalog>/<schema>/<volume>/<path> diretamente
      e remova esta função — não são necessários mounts.
    """
    logger.info("=== Configurando mount points ===")

    # ── AWS S3 (IAM Role — autenticação via Instance Profile) ────────────────
    # Descomente e adapte se estiver na AWS:
    # mount_storage("/mnt/raw",         "s3a://meu-bucket/raw")
    # mount_storage("/mnt/bronze",      "s3a://meu-bucket/bronze")
    # mount_storage("/mnt/silver",      "s3a://meu-bucket/silver")
    # mount_storage("/mnt/gold",        "s3a://meu-bucket/gold")
    # mount_storage("/mnt/checkpoints", "s3a://meu-bucket/checkpoints")
    # mount_storage("/mnt/monitoring",  "s3a://meu-bucket/monitoring")
    # mount_storage("/mnt/audit",       "s3a://meu-bucket/audit")

    # ── Azure ADLS Gen2 (OAuth com Service Principal) ─────────────────────────
    # Descomente e adapte se estiver no Azure:
    # try:
    #     import dbutils
    #     client_secret = dbutils.secrets.get(scope="pipeline-vendas",
    #                                         key="sp-client-secret")
    #     azure_configs = {
    #         "fs.azure.account.auth.type": "OAuth",
    #         "fs.azure.account.oauth.provider.type":
    #             "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    #         "fs.azure.account.oauth2.client.id": "<seu-client-id>",
    #         "fs.azure.account.oauth2.client.secret": client_secret,
    #         "fs.azure.account.oauth2.client.endpoint":
    #             "https://login.microsoftonline.com/<tenant-id>/oauth2/token",
    #     }
    #     mount_storage("/mnt/raw",   "abfss://raw@<storage>.dfs.core.windows.net/",
    #                   azure_configs)
    #     mount_storage("/mnt/bronze","abfss://bronze@<storage>.dfs.core.windows.net/",
    #                   azure_configs)
    #     mount_storage("/mnt/silver","abfss://silver@<storage>.dfs.core.windows.net/",
    #                   azure_configs)
    #     mount_storage("/mnt/gold",  "abfss://gold@<storage>.dfs.core.windows.net/",
    #                   azure_configs)
    # except Exception as e:
    #     logger.error(f"Falha ao configurar Azure mounts: {e}")
    #     raise

    logger.info("[MOUNT] Seção de mounts: adapte os comentários acima ao seu ambiente.")


# =============================================================================
# UNITY CATALOG — SCHEMAS E CATÁLOGO
# =============================================================================

def _is_databricks() -> bool:
    """Detecta se está rodando dentro do Databricks."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def setup_unity_catalog(spark, cfg: PipelineConfig):
    """
    Cria catálogo, schemas e configura o ambiente Unity Catalog.
    Operações são idempotentes (CREATE IF NOT EXISTS).
    Ignorado fora do Databricks — Unity Catalog é exclusivo da plataforma.
    """
    logger.info("=== Configurando Unity Catalog ===")

    if not _is_databricks():
        logger.info("[UC] Fora do Databricks — Unity Catalog ignorado (DDL não suportado localmente).")
        return

    # Catálogo principal
    spark.sql(f"""
        CREATE CATALOG IF NOT EXISTS {cfg.catalog}
        COMMENT 'Catálogo principal do Pipeline E2E de Vendas'
    """)
    spark.sql(f"USE CATALOG {cfg.catalog}")
    logger.info(f"[UC] Catálogo: {cfg.catalog}")

    # Schemas por camada
    schemas = {
        "bronze": "Dados brutos ingeridos — zona de landing",
        "silver": "Dados limpos, validados e enriquecidos",
        "gold":   "Agregações e KPIs prontos para consumo analítico",
    }
    for schema, comment in schemas.items():
        spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{schema}
            COMMENT '{comment}'
        """)
        logger.info(f"[UC] Schema criado/confirmado: {cfg.catalog}.{schema}")


# =============================================================================
# DIRETÓRIOS BASE NO STORAGE
# =============================================================================

def setup_directories(cfg: PipelineConfig):
    """
    Cria os diretórios base no storage cloud.
    No Databricks usa dbutils.fs.mkdirs (operação idempotente).
    """
    logger.info("=== Criando diretórios base ===")

    dirs = [
        cfg.raw_path,
        cfg.bronze_path + "orders/",
        cfg.bronze_path + "order_items/",
        cfg.quarantine_path,
        cfg.silver_path + "orders/",
        cfg.silver_path + "order_items/",
        cfg.rejected_path + "orders/",
        cfg.rejected_path + "order_items/",
        cfg.gold_path + "sales_daily/",
        cfg.gold_path + "sales_weekly/",
        cfg.gold_path + "sales_monthly/",
        cfg.gold_path + "sales_by_category/",
        cfg.gold_path + "kpi_summary/",
        cfg.audit_path,
        cfg.metrics_path,
    ]

    try:
        for d in dirs:
            dbutils.fs.mkdirs(d)  # type: ignore[name-defined]
            logger.info(f"[DIR] ✔ {d}")
    except Exception:
        logger.info("[DIR] dbutils não disponível (fora do Databricks) — diretórios assumidos criados.")

# =============================================================================
# VALIDAÇÕES DE CONECTIVIDADE
# =============================================================================

def validate_connectivity(spark, cfg: PipelineConfig):
    """
    Valida que os caminhos essenciais são acessíveis e que
    as permissões de leitura/escrita estão corretas.
    """
    logger.info("=== Validando conectividade e permissões ===")
    errors = []

    # Testar leitura/escrita no storage (apenas no Databricks — paths são cloud)
    if _is_databricks():
        try:
            files = dbutils.fs.ls(cfg.raw_path)  # type: ignore[name-defined]
            logger.info(f"[VALIDATE] ✔ Raw path acessível: {len(files)} item(s) encontrado(s)")
        except Exception as e:
            msg = f"[VALIDATE] ⚠ Raw path inacessível ({cfg.raw_path}): {e}"
            logger.warning(msg)
            errors.append(msg)

        try:
            test_path = cfg.bronze_path + "_connectivity_test.txt"
            dbutils.fs.put(test_path, "test", overwrite=True)  # type: ignore[name-defined]
            dbutils.fs.rm(test_path)                           # type: ignore[name-defined]
            logger.info("[VALIDATE] ✔ Bronze path com permissão de escrita")
        except Exception as e:
            msg = f"[VALIDATE] ⚠ Sem permissão de escrita em bronze: {e}"
            logger.warning(msg)
            errors.append(msg)
    else:
        logger.info("[VALIDATE] Checks de storage ignorados (execução local — paths são cloud).")

    # Testar Unity Catalog (apenas no Databricks)
    if _is_databricks():
        try:
            spark.sql(f"SHOW SCHEMAS IN {cfg.catalog}").show()
            logger.info(f"[VALIDATE] ✔ Unity Catalog acessível: {cfg.catalog}")
        except Exception as e:
            msg = f"[VALIDATE] ⚠ Unity Catalog inacessível: {e}"
            logger.warning(msg)
            errors.append(msg)
    else:
        logger.info("[VALIDATE] Unity Catalog ignorado (execução local).")

    if errors:
        logger.warning(f"[VALIDATE] {len(errors)} problema(s) encontrado(s). Verifique antes de prosseguir.")
    else:
        logger.info("[VALIDATE] ✔ Todos os checks de conectividade aprovados.")

    return len(errors) == 0


# =============================================================================
# CONFIGURAÇÃO DE SECRETS (documentação)
# =============================================================================

def log_secrets_guide():
    """
    Exibe orientação sobre configuração de secrets.
    Execute estes comandos via Databricks CLI antes de rodar o pipeline.
    """
    guide = """
    ============================================================
    CONFIGURAÇÃO DE SECRETS — Execute via Databricks CLI:
    ============================================================

    # 1. Instalar e configurar a CLI
    pip install databricks-cli
    databricks configure --token
    # (informe: host, token)

    # 2. Criar o scope
    databricks secrets create-scope --scope pipeline-vendas

    # 3. Adicionar secrets necessários
    databricks secrets put --scope pipeline-vendas --key storage-account-key
    databricks secrets put --scope pipeline-vendas --key sp-client-secret
    databricks secrets put --scope pipeline-vendas --key db-connection-string

    # 4. Verificar
    databricks secrets list --scope pipeline-vendas

    # No notebook, acessar via:
    # dbutils.secrets.get(scope="pipeline-vendas", key="storage-account-key")
    ============================================================
    """
    logger.info(guide)


# =============================================================================
# MAIN
# =============================================================================

def main():
    logger.info("=" * 60)
    logger.info("SETUP — Pipeline E2E de Vendas")
    logger.info(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    cfg   = get_config()
    spark = get_spark()

    logger.info(f"Ambiente  : {cfg.env}")
    logger.info(f"Catálogo  : {cfg.catalog}")
    logger.info(f"Batch date: {cfg.batch_date}")

    # 1. Mounts
    setup_mounts(cfg)

    # 2. Unity Catalog
    setup_unity_catalog(spark, cfg)

    # 3. Diretórios
    setup_directories(cfg)

    # 4. Validar conectividade
    ok = validate_connectivity(spark, cfg)

    # 5. Guia de secrets
    log_secrets_guide()

    if ok:
        logger.info("✔ Setup concluído com sucesso — pipeline pronto para execução.")
    else:
        logger.warning("⚠ Setup concluído com avisos — revise os erros acima antes de prosseguir.")

    logger.info("=" * 60)


if __name__ == "__main__":
    main()
