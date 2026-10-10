"""Nettoyage de texte partagé entre entraînement (Sentiment140) et streaming (toots).
Utiliser la MÊME fonction des deux côtés évite le décalage train/inférence."""
from pyspark.sql import Column
from pyspark.sql import functions as F


def clean_text(col: Column) -> Column:
    c = F.regexp_replace(col, r"<br\s*/?>|</p>", " ")          # sauts de ligne HTML
    c = F.regexp_replace(c, r"<[^>]+>", " ")                    # balises HTML (toots)
    c = F.regexp_replace(c, r"&#?\w+;", " ")                    # entités HTML (&amp; &#39;)
    c = F.regexp_replace(c, r"https?://\S+|www\.\S+", " ")      # URLs
    c = F.regexp_replace(c, r"@[\w.]+(@[\w.\-]+)?", " ")        # mentions (@user@instance)
    c = F.lower(c)
    c = F.regexp_replace(c, r"#", "")                           # garde le mot du hashtag
    c = F.regexp_replace(c, r"[^a-z'\s]", " ")                  # lettres uniquement
    c = F.regexp_replace(c, r"\s+", " ")
    return F.trim(c)
