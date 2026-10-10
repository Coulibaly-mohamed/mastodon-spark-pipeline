"""Télécharge Sentiment140 (1,6 M tweets labellisés) dans data/sentiment140/.
Si le lien Stanford ne répond pas : télécharge depuis Kaggle
(https://www.kaggle.com/datasets/kazanova/sentiment140) et dépose le CSV dans data/sentiment140/."""
import sys
import urllib.request
import zipfile
from pathlib import Path

URL = "http://cs.stanford.edu/people/alecmgo/trainingandtestdata.zip"
DEST = Path(__file__).resolve().parent.parent / "data" / "sentiment140"
CSV = DEST / "training.1600000.processed.noemoticon.csv"


def progress(blocks, block_size, total):
    if total > 0:
        pct = min(100, blocks * block_size * 100 // total)
        print(f"\r  {pct:3d} %", end="", flush=True)


def main():
    DEST.mkdir(parents=True, exist_ok=True)
    if CSV.exists():
        print(f"Déjà présent : {CSV}")
        return
    archive = DEST / "trainingandtestdata.zip"
    print(f"Téléchargement de {URL} (~80 Mo)")
    try:
        urllib.request.urlretrieve(URL, archive, progress)
    except Exception as exc:
        sys.exit(f"\nÉchec ({exc}). Télécharge le CSV depuis Kaggle et place-le ici : {CSV}")
    print("\nExtraction...")
    with zipfile.ZipFile(archive) as z:
        z.extract(CSV.name, DEST)
    archive.unlink()
    print(f"OK : {CSV}")


if __name__ == "__main__":
    main()
