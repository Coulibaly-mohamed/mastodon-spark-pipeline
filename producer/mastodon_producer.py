"""
Producer : Mastodon -> Kafka

Modes (variable INGEST_MODE) :
  - stream : API de streaming Mastodon (temps réel)
  - poll   : interrogation régulière de la timeline (fallback robuste)
Source (variable SOURCE) : public | hashtag (liste HASHTAGS)

Si le streaming échoue (instance qui le bloque, réseau...), bascule automatiquement en poll.
"""
import json
import logging
import os
import sys
import threading
import time
from datetime import timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError
from mastodon import Mastodon, StreamListener

logging.basicConfig(level=logging.INFO, format="%(asctime)s [producer] %(message)s")
log = logging.getLogger(__name__)
logging.getLogger("kafka").setLevel(logging.WARNING)

MASTODON_URL = os.getenv("MASTODON_URL", "https://mastodon.social")
MASTODON_TOKEN = os.getenv("MASTODON_TOKEN", "")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "toots")
INGEST_MODE = os.getenv("INGEST_MODE", "stream").lower()
SOURCE = os.getenv("SOURCE", "public").lower()
HASHTAGS = [h.strip().lstrip("#").lower() for h in os.getenv("HASHTAGS", "").split(",") if h.strip()]
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "15"))

sent_count = 0
_lock = threading.Lock()


# --------------------------------------------------------------------------- Kafka
def connect_kafka(retries: int = 30) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks="all",
                linger_ms=50,
            )
            log.info("Connecté à Kafka (%s)", KAFKA_BOOTSTRAP)
            return producer
        except KafkaError as exc:
            log.warning("Kafka indisponible (%d/%d) : %s - nouvel essai dans 5 s", attempt, retries, exc)
            time.sleep(5)
    sys.exit("Impossible de joindre Kafka")


def serialize(status) -> dict:
    """Convertit un statut Mastodon en JSON plat pour Kafka."""
    account = status["account"]
    created = status["created_at"]
    if created.tzinfo is not None:
        created = created.astimezone(timezone.utc)
    return {
        "id": str(status["id"]),
        "created_at": created.strftime("%Y-%m-%d %H:%M:%S"),  # UTC
        "content": status.get("content") or "",
        "language": status.get("language"),
        "account": account.get("acct"),
        "followers_count": int(account.get("followers_count") or 0),
        "reblogs_count": int(status.get("reblogs_count") or 0),
        "favourites_count": int(status.get("favourites_count") or 0),
        "replies_count": int(status.get("replies_count") or 0),
        "hashtags": [t["name"].lower() for t in (status.get("tags") or [])],
        "url": status.get("url"),
    }


def send(producer: KafkaProducer, status) -> None:
    global sent_count
    if status.get("reblog"):  # on ignore les boosts (doublons de contenu)
        return
    msg = serialize(status)
    producer.send(KAFKA_TOPIC, key=msg["id"], value=msg)
    with _lock:
        sent_count += 1
        if sent_count % 50 == 0:
            producer.flush()
            log.info("%d toots envoyés vers '%s'", sent_count, KAFKA_TOPIC)


# --------------------------------------------------------------------------- Streaming
class TootListener(StreamListener):
    def __init__(self, producer):
        super().__init__()
        self.producer = producer

    def on_update(self, status):
        try:
            send(self.producer, status)
        except Exception as exc:  # ne jamais tuer le stream pour un toot mal formé
            log.warning("Toot ignoré : %s", exc)

    def on_abort(self, err):
        log.error("Stream interrompu : %s", err)


def run_stream(api: Mastodon, producer: KafkaProducer) -> None:
    listener = TootListener(producer)
    if SOURCE == "hashtag":
        if not HASHTAGS:
            sys.exit("SOURCE=hashtag mais HASHTAGS est vide")
        log.info("Streaming hashtags : %s", HASHTAGS)
        handles = [api.stream_hashtag(tag, listener, run_async=True, reconnect_async=True) for tag in HASHTAGS]
        while any(h.is_alive() for h in handles):
            time.sleep(10)
        raise RuntimeError("Tous les streams hashtag se sont arrêtés")
    log.info("Streaming timeline publique de %s", MASTODON_URL)
    api.stream_public(listener, run_async=False)


# --------------------------------------------------------------------------- Polling
def run_poll(api: Mastodon, producer: KafkaProducer) -> None:
    sources = [("hashtag", h) for h in HASHTAGS] if SOURCE == "hashtag" else [("public", None)]
    since = {src: None for src in sources}
    log.info("Polling toutes les %d s : %s", POLL_INTERVAL, sources)
    while True:
        for kind, tag in sources:
            try:
                if kind == "hashtag":
                    statuses = api.timeline_hashtag(tag, since_id=since[(kind, tag)], limit=40)
                else:
                    statuses = api.timeline_public(since_id=since[(kind, tag)], limit=40)
            except Exception as exc:
                log.warning("Erreur API (%s) : %s", tag or kind, exc)
                continue
            for status in reversed(statuses):  # du plus ancien au plus récent
                send(producer, status)
            if statuses:
                since[(kind, tag)] = statuses[0]["id"]
        producer.flush()
        time.sleep(POLL_INTERVAL)


# --------------------------------------------------------------------------- Main
def main() -> None:
    if not MASTODON_TOKEN or MASTODON_TOKEN.startswith("colle_"):
        sys.exit("MASTODON_TOKEN manquant : renseigne-le dans le fichier .env")

    api = Mastodon(access_token=MASTODON_TOKEN, api_base_url=MASTODON_URL, request_timeout=30)
    me = api.account_verify_credentials()
    log.info("Authentifié sur %s en tant que @%s", MASTODON_URL, me["acct"])

    producer = connect_kafka()
    try:
        if INGEST_MODE == "stream":
            try:
                run_stream(api, producer)
            except Exception as exc:
                log.error("Streaming indisponible (%s) -> bascule en mode poll", exc)
        run_poll(api, producer)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        log.info("Arrêt. Total envoyé : %d", sent_count)


if __name__ == "__main__":
    main()
