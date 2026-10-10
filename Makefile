# Équivalent de run.ps1 pour Git Bash / WSL / Linux / macOS
EXEC = docker compose exec jupyter

.PHONY: up down reset status logs consume data train stream batch psql count
up:      ; [ -f .env ] || cp .env.example .env ; docker compose up -d --build ; docker compose ps
down:    ; docker compose down
reset:   ; docker compose down -v
status:  ; docker compose ps
logs:    ; docker compose logs -f producer
consume: ; docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic toots --from-beginning --max-messages 5
data:    ; $(EXEC) python scripts/download_sentiment140.py
train:   ; $(EXEC) python ml/train_sentiment.py --nb
stream:  ; $(EXEC) python streaming/stream_processor.py
batch:   ; $(EXEC) python batch/batch_analysis.py
psql:    ; docker compose exec postgres psql -U spark -d mastodon
count:   ; docker compose exec postgres psql -U spark -d mastodon -c "SELECT sentiment, COUNT(*) FROM toots GROUP BY 1;"
