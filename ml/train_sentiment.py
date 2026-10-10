"""
Entraînement du modèle de sentiment (Spark MLlib) sur Sentiment140.

Pipeline : Tokenizer -> StopWordsRemover -> HashingTF -> IDF -> LogisticRegression
Sorties  : ml/models/sentiment_lr/ (PipelineModel) + ml/models/metrics.json

Usage (dans le conteneur jupyter) : python ml/train_sentiment.py [--nb]
  --nb : compare aussi avec un Naive Bayes
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # racine du projet -> 'from common import ...'

import json
import sys
import time

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import IDF, HashingTF, StopWordsRemover, Tokenizer
from pyspark.sql import functions as F

from common import config
from common.text import clean_text

SCHEMA = "target INT, id LONG, date STRING, flag STRING, user STRING, text STRING"


def feature_stages():
    return [
        Tokenizer(inputCol="clean_text", outputCol="words"),
        StopWordsRemover(inputCol="words", outputCol="filtered"),
        HashingTF(inputCol="filtered", outputCol="tf", numFeatures=1 << 18),
        IDF(inputCol="tf", outputCol="features", minDocFreq=5),
    ]


def evaluate(pred, name):
    mc = lambda m: MulticlassClassificationEvaluator(labelCol="label", metricName=m).evaluate(pred)
    metrics = {
        "model": name,
        "accuracy": mc("accuracy"),
        "f1": mc("f1"),
        "precision": mc("weightedPrecision"),
        "recall": mc("weightedRecall"),
        "auc": BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC").evaluate(pred),
    }
    print(f"\n== {name} ==")
    for k, v in metrics.items():
        if k != "model":
            print(f"  {k:<10} {v:.4f}")
    return metrics


def main():
    if not config.SENTIMENT140_CSV.exists():
        sys.exit(
            f"Dataset introuvable : {config.SENTIMENT140_CSV}\n"
            "Lance d'abord : python scripts/download_sentiment140.py"
        )

    spark = get_spark()
    t0 = time.time()

    raw = spark.read.csv(str(config.SENTIMENT140_CSV), schema=SCHEMA, encoding="ISO-8859-1")
    frac = config.TRAIN_SAMPLE_FRACTION
    if frac < 1.0:
        raw = raw.sampleBy("target", fractions={0: frac, 4: frac}, seed=42)

    df = (
        raw.select(
            F.when(F.col("target") == 4, 1.0).otherwise(0.0).alias("label"),
            clean_text(F.col("text")).alias("clean_text"),
        )
        .filter(F.length("clean_text") > 2)
    )
    train, test = df.randomSplit([0.8, 0.2], seed=42)
    train.cache()
    n_train, n_test = train.count(), test.count()
    print(f"Train : {n_train:,} | Test : {n_test:,} (fraction {frac})")

    # -------- Régression logistique (modèle retenu)
    lr = LogisticRegression(maxIter=20, regParam=0.01, elasticNetParam=0.0)
    model = Pipeline(stages=feature_stages() + [lr]).fit(train)
    pred = model.transform(test).cache()
    results = [evaluate(pred, "LogisticRegression")]

    confusion = (
        pred.groupBy("label", "prediction").count()
        .orderBy("label", "prediction").collect()
    )
    confusion = [{"label": int(r["label"]), "prediction": int(r["prediction"]), "count": r["count"]} for r in confusion]

    # -------- Comparaison optionnelle
    if "--nb" in sys.argv:
        nb_model = Pipeline(stages=feature_stages() + [NaiveBayes(modelType="multinomial", smoothing=1.0)]).fit(train)
        results.append(evaluate(nb_model.transform(test), "NaiveBayes"))

    model.write().overwrite().save(config.MODEL_PATH)

    payload = {
        "train_rows": n_train,
        "test_rows": n_test,
        "sample_fraction": frac,
        "training_seconds": round(time.time() - t0, 1),
        "results": results,
        "confusion_matrix": confusion,
    }
    config.METRICS_PATH.write_text(json.dumps(payload, indent=2))
    print(f"\nModèle sauvegardé : {config.MODEL_PATH}")
    print(f"Métriques : {config.METRICS_PATH}")

    # -------- Sanity check
    samples = spark.createDataFrame(
        [("I love this new feature, great job!",), ("This is the worst update ever, so disappointed",),
         ("Spark streaming is awesome",), ("I hate waiting for the bus in the rain",)],
        ["text"],
    ).withColumn("clean_text", clean_text(F.col("text")))
    model.transform(samples).select("text", "prediction").show(truncate=False)
    spark.stop()


def get_spark():
    return config.get_spark("train-sentiment", kafka=False, postgres=False, driver_memory="3g")


if __name__ == "__main__":
    main()
