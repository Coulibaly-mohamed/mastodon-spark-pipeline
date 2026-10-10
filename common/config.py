"""Configuration partagée (lue depuis les variables d'environnement / .env)."""
import os
from pathlib import Path

try:  # charge .env si présent (utile hors Docker)
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:
    pass

BASE_DIR = Path(__file__).resolve().parent.parent

# Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "toots")

# PostgreSQL
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "mastodon")
PG_USER = os.getenv("POSTGRES_USER", "spark")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "spark")
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}
SQLALCHEMY_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# Chemins
DATA_DIR = BASE_DIR / "data" / "sentiment140"
SENTIMENT140_CSV = DATA_DIR / "training.1600000.processed.noemoticon.csv"
MODEL_PATH = str(BASE_DIR / "ml" / "models" / "sentiment_lr")
METRICS_PATH = BASE_DIR / "ml" / "models" / "metrics.json"
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", str(BASE_DIR / "checkpoints"))
OUTPUT_DIR = BASE_DIR / "outputs"

# Traitement
FILTER_LANG = os.getenv("FILTER_LANG", "en")
TRAIN_SAMPLE_FRACTION = float(os.getenv("TRAIN_SAMPLE_FRACTION", "0.3"))
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "2g")

POSTGRES_JAR = "org.postgresql:postgresql:42.7.3"


def _kafka_package() -> str:
    """Connecteur Kafka aligné sur la version de PySpark installée."""
    import pyspark
    version = pyspark.__version__.split("+")[0]
    scala = "2.13" if version.startswith("4") else "2.12"
    return f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{version}"


def get_spark(app_name: str, kafka: bool = False, postgres: bool = True,
              driver_memory: str | None = None, ui_port: int = 4040):
    """Crée une SparkSession locale avec les connecteurs nécessaires."""
    from pyspark.sql import SparkSession

    packages = []
    if kafka:
        packages.append(_kafka_package())
    if postgres:
        packages.append(POSTGRES_JAR)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", driver_memory or SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.port", str(ui_port))
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    )
    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def pg_connect():
    """Connexion psycopg2 (requêtes SQL directes)."""
    import psycopg2
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)


def pg_execute(sql: str) -> int:
    """Exécute une requête, commit, ferme la connexion. Retourne rowcount."""
    conn = pg_connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql)
            return cur.rowcount
    finally:
        conn.close()
