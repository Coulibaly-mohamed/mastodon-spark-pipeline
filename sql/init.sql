-- Exécuté automatiquement au premier démarrage du conteneur PostgreSQL

-- Toots enrichis (sortie du streaming)
CREATE TABLE IF NOT EXISTS toots (
    id                TEXT PRIMARY KEY,
    created_at        TIMESTAMP NOT NULL,
    account           TEXT,
    language          TEXT,
    content           TEXT,
    clean_text        TEXT,
    hashtags          TEXT,             -- "ai,python,data"
    followers_count   BIGINT,
    reblogs_count     BIGINT,
    favourites_count  BIGINT,
    replies_count     BIGINT,
    sentiment         TEXT,             -- positive | negative
    score_pos         DOUBLE PRECISION, -- probabilité "positive"
    ingested_at       TIMESTAMP DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_toots_created_at ON toots (created_at);
CREATE INDEX IF NOT EXISTS idx_toots_sentiment  ON toots (sentiment);

-- Table tampon : Spark y écrit chaque micro-batch, puis INSERT ... ON CONFLICT dans toots (dédoublonnage)
CREATE TABLE IF NOT EXISTS toots_staging (LIKE toots INCLUDING DEFAULTS);

-- Agrégats fenêtrés (5 min) produits en streaming
CREATE TABLE IF NOT EXISTS sentiment_window (
    window_start  TIMESTAMP,
    window_end    TIMESTAMP,
    sentiment     TEXT,
    nb_toots      BIGINT,
    avg_score     DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_sw_start ON sentiment_window (window_start);

CREATE TABLE IF NOT EXISTS hashtag_window (
    window_start    TIMESTAMP,
    window_end      TIMESTAMP,
    hashtag         TEXT,
    nb_occurrences  BIGINT
);
CREATE INDEX IF NOT EXISTS idx_hw_start ON hashtag_window (window_start);

-- Les tables batch_* sont créées par batch/batch_analysis.py (mode overwrite)
