"""
Spark Structured Streaming : Kafka -> nettoyage -> sentiment (MLlib) -> PostgreSQL

3 requêtes en parallèle :
  1. toots            : chaque toot enrichi (dédoublonné via ON CONFLICT)
  2. sentiment_window : nb de toots par sentiment, fenêtres de 5 min (watermark 10 min)
  3. hashtag_window   : occurrences de hashtags, fenêtres de 5 min

Usage (dans le conteneur jupyter) : python streaming/stream_processor.py
Arrêt : Ctrl+C (les checkpoints permettent de reprendre là où on s'est arrêté).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # racine du projet -> 'from common import ...'

import os
import sys

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

from common import config
from common.text import clean_text

TRIGGER = os.getenv("STREAM_TRIGGER", "30 seconds")
WINDOW = os.getenv("STREAM_WINDOW", "5 minutes")
WATERMARK = os.getenv("STREAM_WATERMARK", "10 minutes")

TOOT_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("created_at", StringType()),
    StructField("content", StringType()),
    StructField("language", StringType()),
    StructField("account", StringType()),
    StructField("followers_count", LongType()),
    StructField("reblogs_count", LongType()),
    StructField("favourites_count", LongType()),
    StructField("replies_count", LongType()),
    StructField("hashtags", ArrayType(StringType())),
    StructField("url", StringType()),
])

TOOT_COLUMNS = [
    "id", "created_at", "account", "language", "content", "clean_text", "hashtags",
    "followers_count", "reblogs_count", "favourites_count", "replies_count", "sentiment", "score_pos",
]


# --------------------------------------------------------------------------- Sinks
def write_toots(batch_df, batch_id):
    """Micro-batch -> table tampon -> INSERT ... ON CONFLICT DO NOTHING (idempotent)."""
    rows = batch_df.dropDuplicates(["id"]).select(*TOOT_COLUMNS).cache()
    n = rows.count()
    if n == 0:
        rows.unpersist()
        return
    config.pg_execute("TRUNCATE toots_staging")
    rows.write.jdbc(config.JDBC_URL, "toots_staging", mode="append", properties=config.JDBC_PROPS)
    cols = ", ".join(TOOT_COLUMNS)
    inserted = config.pg_execute(
        f"INSERT INTO toots ({cols}) SELECT {cols} FROM toots_staging ON CONFLICT (id) DO NOTHING"
    )
    stats = {r["sentiment"]: r["count"] for r in rows.groupBy("sentiment").count().collect()}
    print(f"[batch {batch_id}] toots reçus={n} insérés={inserted} sentiment={stats}", flush=True)
    rows.unpersist()


def jdbc_append(table):
    def _write(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        batch_df.write.jdbc(config.JDBC_URL, table, mode="append", properties=config.JDBC_PROPS)
        print(f"[batch {batch_id}] {table} : {batch_df.count()} lignes", flush=True)
    return _write


# --------------------------------------------------------------------------- Pipeline
def main():
    if not os.path.isdir(config.MODEL_PATH):
        sys.exit(f"Modèle introuvable ({config.MODEL_PATH}). Lance d'abord : python ml/train_sentiment.py")

    spark = config.get_spark("mastodon-streaming", kafka=True, postgres=True, ui_port=4040)
    model = PipelineModel.load(config.MODEL_PATH)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 5000)
        .load()
    )

    toots = (
        raw.select(F.from_json(F.col("value").cast("string"), TOOT_SCHEMA).alias("t"))
        .select("t.*")
        .filter(F.col("id").isNotNull())
        .withColumn("created_at", F.to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("clean_text", clean_text(F.col("content")))
        .withColumn("tags", F.coalesce(F.col("hashtags"), F.array().cast("array<string>")))
        .withColumn("hashtags", F.concat_ws(",", "tags"))
    )
    if config.FILTER_LANG:
        toots = toots.filter(F.col("language") == config.FILTER_LANG)
    toots = toots.filter(F.length("clean_text") > 2)

    scored = (
        model.transform(toots)
        .withColumn("sentiment", F.when(F.col("prediction") == 1.0, "positive").otherwise("negative"))
        .withColumn("score_pos", vector_to_array("probability")[1])
    )

    ckpt = config.CHECKPOINT_DIR

    # 1. Toots enrichis
    q_toots = (
        scored.writeStream.foreachBatch(write_toots)
        .option("checkpointLocation", f"{ckpt}/toots")
        .trigger(processingTime=TRIGGER)
        .queryName("toots")
        .start()
    )

    # 2. Sentiment par fenêtre
    sentiment_win = (
        scored.withWatermark("created_at", WATERMARK)
        .groupBy(F.window("created_at", WINDOW), "sentiment")
        .agg(F.count("*").alias("nb_toots"), F.avg("score_pos").alias("avg_score"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "sentiment", "nb_toots", "avg_score",
        )
    )
    q_sent = (
        sentiment_win.writeStream.outputMode("append")
        .foreachBatch(jdbc_append("sentiment_window"))
        .option("checkpointLocation", f"{ckpt}/sentiment_window")
        .trigger(processingTime=TRIGGER)
        .queryName("sentiment_window")
        .start()
    )

    # 3. Hashtags par fenêtre
    hashtag_win = (
        toots.select("created_at", F.explode("tags").alias("hashtag"))
        .withWatermark("created_at", WATERMARK)
        .groupBy(F.window("created_at", WINDOW), "hashtag")
        .agg(F.count("*").alias("nb_occurrences"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "hashtag", "nb_occurrences",
        )
    )
    q_tags = (
        hashtag_win.writeStream.outputMode("append")
        .foreachBatch(jdbc_append("hashtag_window"))
        .option("checkpointLocation", f"{ckpt}/hashtag_window")
        .trigger(processingTime=TRIGGER)
        .queryName("hashtag_window")
        .start()
    )

    print(f"Streaming démarré : topic '{config.KAFKA_TOPIC}' -> PostgreSQL. Spark UI : http://localhost:4040", flush=True)
    print("Note : les fenêtres n'apparaissent en base qu'une fois fermées (fenêtre + watermark ≈ 15 min).", flush=True)
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("Arrêt demandé...")
    finally:
        for q in (q_toots, q_sent, q_tags):
            q.stop()
        spark.stop()


if __name__ == "__main__":
    main()
