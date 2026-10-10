# Commandes du projet (Windows PowerShell)
# Usage : .\run.ps1 <commande>
# Si bloqué par la politique d'exécution : powershell -ExecutionPolicy Bypass -File .\run.ps1 <commande>
param([Parameter(Position = 0)][string]$cmd = "help")

$exec = "docker compose exec jupyter"

switch ($cmd) {
    "up"       { if (-not (Test-Path .env)) { Copy-Item .env.example .env; Write-Host ".env créé : renseigne MASTODON_TOKEN puis relance '.\run.ps1 up'"; exit 1 }
                 docker compose up -d --build; docker compose ps }
    "down"     { docker compose down }
    "reset"    { docker compose down -v }          # supprime aussi les données (Kafka, Postgres, checkpoints)
    "status"   { docker compose ps }
    "logs"     { docker compose logs -f producer }
    "consume"  { docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic toots --from-beginning --max-messages 5 }
    "data"     { Invoke-Expression "$exec python scripts/download_sentiment140.py" }
    "train"    { Invoke-Expression "$exec python ml/train_sentiment.py --nb" }
    "stream"   { Invoke-Expression "$exec python streaming/stream_processor.py" }
    "batch"    { Invoke-Expression "$exec python batch/batch_analysis.py" }
    "psql"     { docker compose exec postgres psql -U spark -d mastodon }
    "count"    { docker compose exec postgres psql -U spark -d mastodon -c "SELECT sentiment, COUNT(*) FROM toots GROUP BY 1;" }
    default    {
        Write-Host @"
Commandes :
  up       Démarre Kafka, PostgreSQL, Kafka UI, producer, Jupyter
  status   État des conteneurs
  logs     Logs du producer (Mastodon -> Kafka)
  consume  Affiche 5 messages du topic Kafka
  data     Télécharge Sentiment140
  train    Entraîne le modèle de sentiment (LR + comparaison NB)
  stream   Lance Spark Structured Streaming (Kafka -> PostgreSQL)
  batch    Lance l'analyse batch (PostgreSQL -> tables batch_*)
  count    Nb de toots en base par sentiment
  psql     Console PostgreSQL
  down     Arrête tout (données conservées)
  reset    Arrête tout et SUPPRIME les données
URLs : Jupyter http://localhost:8888 (token: spark) | Kafka UI http://localhost:8080 | Spark UI http://localhost:4040
"@
    }
}
