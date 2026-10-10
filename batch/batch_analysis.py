"""
Analyse batch (Spark) sur l'historique stocké dans PostgreSQL.

Lit la table `toots`, calcule les indicateurs, et écrit :
  - les tables batch_* dans PostgreSQL (lues par les notebooks)
  - des CSV dans outputs/

Usage (dans le conteneur jupyter) : python batch/batch_analysis.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # racine du projet -> 'from common import ...'

import sys

from pyspark.sql import functions as F

from common import config


def save(df, name):
    df.write.jdbc(config.JDBC_URL, name, mode="overwrite", properties=config.JDBC_PROPS)
    config.OUTPUT_DIR.mkdir(exist_ok=True)
    df.toPandas().to_csv(config.OUTPUT_DIR / f"{name}.csv", index=False)
    print(f"\n== {name} ==")
    df.show(10, truncate=False)


def main():
    spark = config.get_spark("mastodon-batch", kafka=False, postgres=True, ui_port=4041)

    toots = spark.read.jdbc(config.JDBC_URL, "toots", properties=config.JDBC_PROPS).cache()
    total = toots.count()
    if total == 0:
        sys.exit("Table toots vide : laisse tourner le producer + le streaming un moment.")
    period = toots.agg(F.min("created_at").alias("debut"), F.max("created_at").alias("fin")).first()
    print(f"{total:,} toots du {period['debut']} au {period['fin']} (UTC)")

    toots = (
        toots.withColumn("tags", F.split(F.col("hashtags"), ","))
        .withColumn("nb_words", F.size(F.split("clean_text", " ")))
        .withColumn("engagement", F.col("reblogs_count") + F.col("favourites_count") + F.col("replies_count"))
    )
    toots.createOrReplaceTempView("toots")

    # 1. Répartition globale du sentiment (Spark SQL)
    save(spark.sql("""
        SELECT sentiment,
               COUNT(*)                                       AS nb_toots,
               ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
               ROUND(AVG(score_pos), 3)                       AS avg_score_pos
        FROM toots GROUP BY sentiment ORDER BY sentiment
    """), "batch_sentiment_global")

    # 2. Volume et sentiment par heure
    save(
        toots.groupBy(F.date_trunc("hour", "created_at").alias("hour"))
        .agg(
            F.count("*").alias("nb_toots"),
            F.sum(F.when(F.col("sentiment") == "positive", 1).otherwise(0)).alias("nb_positive"),
            F.sum(F.when(F.col("sentiment") == "negative", 1).otherwise(0)).alias("nb_negative"),
            F.round(F.avg("score_pos"), 3).alias("avg_score_pos"),
        )
        .withColumn("pct_positive", F.round(100 * F.col("nb_positive") / F.col("nb_toots"), 2))
        .orderBy("hour"),
        "batch_hourly",
    )

    # 3. Top hashtags + part de positif
    tags = toots.select(F.explode("tags").alias("hashtag"), "sentiment", "engagement").filter(F.col("hashtag") != "")
    save(
        tags.groupBy("hashtag")
        .agg(
            F.count("*").alias("nb_toots"),
            F.round(100 * F.avg(F.when(F.col("sentiment") == "positive", 1).otherwise(0)), 2).alias("pct_positive"),
            F.round(F.avg("engagement"), 2).alias("avg_engagement"),
        )
        .orderBy(F.desc("nb_toots"))
        .limit(30),
        "batch_top_hashtags",
    )

    # 4. Comptes les plus actifs
    save(
        toots.groupBy("account")
        .agg(F.count("*").alias("nb_toots"), F.max("followers_count").alias("followers"),
             F.round(F.avg("score_pos"), 3).alias("avg_score_pos"))
        .orderBy(F.desc("nb_toots"))
        .limit(20),
        "batch_top_accounts",
    )

    # 5. Engagement & longueur par sentiment
    save(
        toots.groupBy("sentiment")
        .agg(
            F.round(F.avg("reblogs_count"), 2).alias("avg_reblogs"),
            F.round(F.avg("favourites_count"), 2).alias("avg_favourites"),
            F.round(F.avg("replies_count"), 2).alias("avg_replies"),
            F.round(F.avg("engagement"), 2).alias("avg_engagement"),
            F.round(F.avg("nb_words"), 1).alias("avg_words"),
            F.round(F.avg("followers_count"), 0).alias("avg_followers"),
        ),
        "batch_engagement",
    )

    # 6. Heatmap d'activité (jour x heure)
    save(
        toots.groupBy(
            F.date_format("created_at", "E").alias("day"),
            F.dayofweek("created_at").alias("day_num"),
            F.hour("created_at").alias("hour"),
        ).count().orderBy("day_num", "hour"),
        "batch_activity",
    )

    # 7. Corrélation engagement <-> score de sentiment
    corr = toots.stat.corr("engagement", "score_pos")
    print(f"\nCorrélation (Pearson) engagement / score positif : {corr:.4f}")
    save(spark.createDataFrame([("engagement_vs_score_pos", float(corr), total)],
                               ["metric", "value", "nb_toots"]), "batch_correlation")

    spark.stop()


if __name__ == "__main__":
    main()
