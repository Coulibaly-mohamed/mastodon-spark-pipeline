# Mastodon Data Analysis & Processing with Apache Spark

Pipeline end-to-end : ingestion temps réel de *toots* Mastodon via **Kafka**, traitement **streaming + batch** avec **Apache Spark**, analyse de sentiment avec **Spark MLlib**, stockage **PostgreSQL**, visualisations dans **Jupyter**.

## Architecture

```drawio
<mxfile>
  <diagram id="LvA3C735_2Nddq6Wjm3t" name="Page-1">
    <mxGraphModel dx="667" dy="695" grid="1" gridSize="10" guides="1" tooltips="0" connect="1" arrows="1" fold="1" page="1" pageScale="1" pageWidth="827" pageHeight="1169" math="0" shadow="0">
      <root>
        <mxCell id="yTP9tqG_ZDtt-V5R-cQw-0" />
        <mxCell id="yTP9tqG_ZDtt-V5R-cQw-1" parent="yTP9tqG_ZDtt-V5R-cQw-0" />
        <UserObject label="" mermaidData="{&#xa;  &quot;data&quot;: &quot;flowchart LR\n    A[Mastodon API] --&gt;|Mastodon.py| B[producer]\n    B --&gt;|JSON| C[(Kafka · topic toots)]\n    C --&gt; D[Spark Structured Streaming&lt;br/&gt;nettoyage · sentiment · fenêtres 5 min]\n    D --&gt;|foreachBatch JDBC| E[(PostgreSQL)]\n    F[Sentiment140] --&gt; G[Spark MLlib&lt;br/&gt;TF-IDF + LogisticRegression]\n    G --&gt;|PipelineModel| D\n    E --&gt; H[Spark Batch&lt;br/&gt;tables batch_*]\n    H --&gt; I[Jupyter&lt;br/&gt;Matplotlib · Seaborn]&quot;,&#xa;  &quot;config&quot;: null&#xa;}" id="leXPLPGI7J2cDm4CVkzy-0">
          <mxCell connectable="0" parent="yTP9tqG_ZDtt-V5R-cQw-1" style="group;transparentBounds=1;editIcon=1;lockedGroup=0;groupPadding=10;" vertex="1">
            <mxGeometry as="geometry" />
          </mxCell>
        </UserObject>
        <UserObject label="Mastodon API" mermaidId="n:A" mermaidBaseStyle="html=1;whiteSpace=wrap;strokeWidth=1;fillColor=light-dark(#ECECFF,#1f2020);strokeColor=light-dark(#9370DB,#cccccc);fontColor=light-dark(#333333,#cccccc);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontSize=16;" mermaidBaseValue="Mastodon API" id="yTP9tqG_ZDtt-V5R-cQw-2">
          <mxCell parent="leXPLPGI7J2cDm4CVkzy-0" style="html=1;whiteSpace=wrap;strokeWidth=1;fillColor=light-dark(#ECECFF,#1f2020);strokeColor=light-dark(#9370DB,#cccccc);fontColor=light-dark(#333333,#cccccc);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontSize=16;" vertex="1">
            <mxGeometry height="54" width="154" x="10" y="17" as="geometry" />
          </mxCell>
        </UserObject>
        <UserObject label="producer" mermaidId="n:B" mermaidBaseStyle="html=1;whiteSpace=wrap;strokeWidth=1;fillColor=light-dark(#ECECFF,#1f2020);strokeColor=light-dark(#9370DB,#cccccc);fontColor=light-dark(#333333,#cccccc);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontSize=16;" mermaidBaseValue="producer" id="yTP9tqG_ZDtt-V5R-cQw-3">
          <mxCell parent="leXPLPGI7J2cDm4CVkzy-0" style="html=1;whiteSpace=wrap;strokeWidth=1;fillColor=light-dark(#ECECFF,#1f2020);strokeColor=light-dark(#9370DB,#cccccc);fontColor=light-dark(#333333,#cccccc);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontSize=16;" vertex="1">
            <mxGeometry height="54" width="124" x="321" y="17" as="geometry" />
          </mxCell>
        </UserObject>
        <UserObject label="Kafka · topic toots" mermaidId="n:C" mermaidBaseStyle="html=1;shape=cylinder3;boundedLbl=1;backgroundOutline=1;size=10;strokeWidth=1;whiteSpace=wrap;fillColor=light-dark(#ECECFF,#1f2020);strokeColor=light-dark(#9370DB,#cccccc);fontColor=light-dark(#333333,#cccccc);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontSize=16;" mermaidBaseValue="Kafka · topic toots" id="yTP9tqG_ZDtt-V5R-cQw-4">
          <mxCell parent="leXPLPGI7J2cDm4CVkzy-0" style="html=1;shape=cylinder3;boundedLbl=1;backgroundOutline=1;size=10;strokeWidth=1;whiteSpace=wrap;fillColor=light-dark(#ECECFF,#1f2020);strokeColor=light-dark(#9370DB,#cccccc);fontColor=light-dark(#333333,#cccccc);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontSize=16;" vertex="1">
            <mxGeometry height="67" width="148" x="601" y="10" as="geometry" />
          </mxCell>
        </UserObject>
        <UserObject label="Spark Structured Streaming&#xa;nettoyage · sentiment · fenêtres 5 min" mermaidId="n:D" mermaidBaseStyle="html=1;whiteSpace=wrap;strokeWidth=1;fillColor=light-dark(#ECECFF,#1f2020);strokeColor=light-dark(#9370DB,#cccccc);fontColor=light-dark(#333333,#cccccc);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontSize=16;" mermaidBaseValue="Spark Structured Streaming&#xa;nettoyage · sentiment · fenêtres 5 min" id="yTP9tqG_ZDtt-V5R-cQw-5">
          <mxCell parent="leXPLPGI7J2cDm4CVkzy-0" style="html=1;whiteSpace=wrap;strokeWidth=1;fillColor=light-dark(#ECECFF,#1f2020);strokeColor=light-dark(#9370DB,#cccccc);fontColor=light-dark(#333333,#cccccc);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontSize=16;" vertex="1">
            <mxGeometry height="73" width="339" x="951" y="67" as="geometry" />
          </mxCell>
        </UserObject>
        <UserObject label="PostgreSQL" mermaidId="n:E" mermaidBaseStyle="html=1;shape=cylinder3;boundedLbl=1;backgroundOutline=1;size=10;strokeWidth=1;whiteSpace=wrap;fillColor=light-dark(#ECECFF,#1f2020);strokeColor=light-dark(#9370DB,#cccccc);fontColor=light-dark(#333333,#cccccc);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontSize=16;" mermaidBaseValue="PostgreSQL" id="yTP9tqG_ZDtt-V5R-cQw-6">
          <mxCell parent="leXPLPGI7J2cDm4CVkzy-0" style="html=1;shape=cylinder3;boundedLbl=1;backgroundOutline=1;size=10;strokeWidth=1;whiteSpace=wrap;fillColor=light-dark(#ECECFF,#1f2020);strokeColor=light-dark(#9370DB,#cccccc);fontColor=light-dark(#333333,#cccccc);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontSize=16;" vertex="1">
            <mxGeometry height="67" width="94" x="1476" y="70" as="geometry" />
          </mxCell>
        </UserObject>
        <UserObject label="Sentiment140" mermaidId="n:F" mermaidBaseStyle="html=1;whiteSpace=wrap;strokeWidth=1;fillColor=light-dark(#ECECFF,#1f2020);strokeColor=light-dark(#9370DB,#cccccc);fontColor=light-dark(#333333,#cccccc);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontSize=16;" mermaidBaseValue="Sentiment140" id="yTP9tqG_ZDtt-V5R-cQw-7">
          <mxCell parent="leXPLPGI7J2cDm4CVkzy-0" style="html=1;whiteSpace=wrap;strokeWidth=1;fillColor=light-dark(#ECECFF,#1f2020);strokeColor=light-dark(#9370DB,#cccccc);fontColor=light-dark(#333333,#cccccc);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontSize=16;" vertex="1">
            <mxGeometry height="54" width="158" x="304" y="137" as="geometry" />
          </mxCell>
        </UserObject>
        <UserObject label="Spark MLlib&#xa;TF-IDF + LogisticRegression" mermaidId="n:G" mermaidBaseStyle="html=1;whiteSpace=wrap;strokeWidth=1;fillColor=light-dark(#ECECFF,#1f2020);strokeColor=light-dark(#9370DB,#cccccc);fontColor=light-dark(#333333,#cccccc);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontSize=16;" mermaidBaseValue="Spark MLlib&#xa;TF-IDF + LogisticRegression" id="yTP9tqG_ZDtt-V5R-cQw-8">
          <mxCell parent="leXPLPGI7J2cDm4CVkzy-0" style="html=1;whiteSpace=wrap;strokeWidth=1;fillColor=light-dark(#ECECFF,#1f2020);strokeColor=light-dark(#9370DB,#cccccc);fontColor=light-dark(#333333,#cccccc);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontSize=16;" vertex="1">
            <mxGeometry height="73" width="254" x="548" y="127" as="geometry" />
          </mxCell>
        </UserObject>
        <UserObject label="Spark Batch&#xa;tables batch_*" mermaidId="n:H" mermaidBaseStyle="html=1;whiteSpace=wrap;strokeWidth=1;fillColor=light-dark(#ECECFF,#1f2020);strokeColor=light-dark(#9370DB,#cccccc);fontColor=light-dark(#333333,#cccccc);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontSize=16;" mermaidBaseValue="Spark Batch&#xa;tables batch_*" id="yTP9tqG_ZDtt-V5R-cQw-9">
          <mxCell parent="leXPLPGI7J2cDm4CVkzy-0" style="html=1;whiteSpace=wrap;strokeWidth=1;fillColor=light-dark(#ECECFF,#1f2020);strokeColor=light-dark(#9370DB,#cccccc);fontColor=light-dark(#333333,#cccccc);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontSize=16;" vertex="1">
            <mxGeometry height="73" width="163" x="1620" y="67" as="geometry" />
          </mxCell>
        </UserObject>
        <UserObject label="Jupyter&#xa;Matplotlib · Seaborn" mermaidId="n:I" mermaidBaseStyle="html=1;whiteSpace=wrap;strokeWidth=1;fillColor=light-dark(#ECECFF,#1f2020);strokeColor=light-dark(#9370DB,#cccccc);fontColor=light-dark(#333333,#cccccc);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontSize=16;" mermaidBaseValue="Jupyter&#xa;Matplotlib · Seaborn" id="yTP9tqG_ZDtt-V5R-cQw-10">
          <mxCell parent="leXPLPGI7J2cDm4CVkzy-0" style="html=1;whiteSpace=wrap;strokeWidth=1;fillColor=light-dark(#ECECFF,#1f2020);strokeColor=light-dark(#9370DB,#cccccc);fontColor=light-dark(#333333,#cccccc);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontSize=16;" vertex="1">
            <mxGeometry height="73" width="206" x="1833" y="67" as="geometry" />
          </mxCell>
        </UserObject>
        <UserObject label="Mastodon.py" mermaidId="e:A-&gt;B#0" mermaidBaseStyle="curved=1;startArrow=none;endArrow=block;endSize=7;strokeColor=light-dark(#333333,#cccccc);html=1;fontSize=16;labelBackgroundColor=light-dark(#E8E8E88D,#2a2a2a8D);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontColor=light-dark(#333333,#cccccc);exitX=1;exitY=0.5;entryX=0;entryY=0.5;" mermaidBaseValue="Mastodon.py" id="yTP9tqG_ZDtt-V5R-cQw-11">
          <mxCell edge="1" parent="leXPLPGI7J2cDm4CVkzy-0" source="yTP9tqG_ZDtt-V5R-cQw-2" style="curved=1;startArrow=none;endArrow=block;endSize=7;strokeColor=light-dark(#333333,#cccccc);html=1;fontSize=16;labelBackgroundColor=light-dark(#E8E8E88D,#2a2a2a8D);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontColor=light-dark(#333333,#cccccc);exitX=1;exitY=0.5;entryX=0;entryY=0.5;" target="yTP9tqG_ZDtt-V5R-cQw-3">
            <mxGeometry relative="1" as="geometry">
              <Array as="points" />
            </mxGeometry>
          </mxCell>
        </UserObject>
        <UserObject label="JSON" mermaidId="e:B-&gt;C#0" mermaidBaseStyle="curved=1;startArrow=none;endArrow=block;endSize=7;strokeColor=light-dark(#333333,#cccccc);html=1;fontSize=16;labelBackgroundColor=light-dark(#E8E8E88D,#2a2a2a8D);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontColor=light-dark(#333333,#cccccc);exitX=1;exitY=0.5;entryX=0;entryY=0.51;" mermaidBaseValue="JSON" id="yTP9tqG_ZDtt-V5R-cQw-12">
          <mxCell edge="1" parent="leXPLPGI7J2cDm4CVkzy-0" source="yTP9tqG_ZDtt-V5R-cQw-3" style="curved=1;startArrow=none;endArrow=block;endSize=7;strokeColor=light-dark(#333333,#cccccc);html=1;fontSize=16;labelBackgroundColor=light-dark(#E8E8E88D,#2a2a2a8D);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontColor=light-dark(#333333,#cccccc);exitX=1;exitY=0.5;entryX=0;entryY=0.51;" target="yTP9tqG_ZDtt-V5R-cQw-4">
            <mxGeometry relative="1" as="geometry">
              <Array as="points" />
            </mxGeometry>
          </mxCell>
        </UserObject>
        <UserObject label="" mermaidId="e:C-&gt;D#0" mermaidBaseStyle="curved=1;startArrow=none;endArrow=block;endSize=7;strokeColor=light-dark(#333333,#cccccc);exitX=1;exitY=0.51;entryX=0.06;entryY=0;" mermaidBaseValue="" id="yTP9tqG_ZDtt-V5R-cQw-13">
          <mxCell edge="1" parent="leXPLPGI7J2cDm4CVkzy-0" source="yTP9tqG_ZDtt-V5R-cQw-4" style="curved=1;startArrow=none;endArrow=block;endSize=7;strokeColor=light-dark(#333333,#cccccc);exitX=1;exitY=0.51;entryX=0.06;entryY=0;" target="yTP9tqG_ZDtt-V5R-cQw-5">
            <mxGeometry relative="1" as="geometry">
              <Array as="points">
                <mxPoint x="877" y="44" />
              </Array>
            </mxGeometry>
          </mxCell>
        </UserObject>
        <UserObject label="foreachBatch JDBC" mermaidId="e:D-&gt;E#0" mermaidBaseStyle="curved=1;startArrow=none;endArrow=block;endSize=7;strokeColor=light-dark(#333333,#cccccc);html=1;fontSize=16;labelBackgroundColor=light-dark(#E8E8E88D,#2a2a2a8D);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontColor=light-dark(#333333,#cccccc);exitX=1;exitY=0.51;entryX=0;entryY=0.51;" mermaidBaseValue="foreachBatch JDBC" id="yTP9tqG_ZDtt-V5R-cQw-14">
          <mxCell edge="1" parent="leXPLPGI7J2cDm4CVkzy-0" source="yTP9tqG_ZDtt-V5R-cQw-5" style="curved=1;startArrow=none;endArrow=block;endSize=7;strokeColor=light-dark(#333333,#cccccc);html=1;fontSize=16;labelBackgroundColor=light-dark(#E8E8E88D,#2a2a2a8D);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontColor=light-dark(#333333,#cccccc);exitX=1;exitY=0.51;entryX=0;entryY=0.51;" target="yTP9tqG_ZDtt-V5R-cQw-6">
            <mxGeometry relative="1" as="geometry">
              <Array as="points" />
            </mxGeometry>
          </mxCell>
        </UserObject>
        <UserObject label="" mermaidId="e:F-&gt;G#0" mermaidBaseStyle="curved=1;startArrow=none;endArrow=block;endSize=7;strokeColor=light-dark(#333333,#cccccc);exitX=1;exitY=0.5;entryX=0;entryY=0.51;" mermaidBaseValue="" id="yTP9tqG_ZDtt-V5R-cQw-15">
          <mxCell edge="1" parent="leXPLPGI7J2cDm4CVkzy-0" source="yTP9tqG_ZDtt-V5R-cQw-7" style="curved=1;startArrow=none;endArrow=block;endSize=7;strokeColor=light-dark(#333333,#cccccc);exitX=1;exitY=0.5;entryX=0;entryY=0.51;" target="yTP9tqG_ZDtt-V5R-cQw-8">
            <mxGeometry relative="1" as="geometry">
              <Array as="points" />
            </mxGeometry>
          </mxCell>
        </UserObject>
        <UserObject label="PipelineModel" mermaidId="e:G-&gt;D#0" mermaidBaseStyle="curved=1;startArrow=none;endArrow=block;endSize=7;strokeColor=light-dark(#333333,#cccccc);html=1;fontSize=16;labelBackgroundColor=light-dark(#E8E8E88D,#2a2a2a8D);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontColor=light-dark(#333333,#cccccc);exitX=1;exitY=0.51;entryX=0.06;entryY=1;" mermaidBaseValue="PipelineModel" id="yTP9tqG_ZDtt-V5R-cQw-16">
          <mxCell edge="1" parent="leXPLPGI7J2cDm4CVkzy-0" source="yTP9tqG_ZDtt-V5R-cQw-8" style="curved=1;startArrow=none;endArrow=block;endSize=7;strokeColor=light-dark(#333333,#cccccc);html=1;fontSize=16;labelBackgroundColor=light-dark(#E8E8E88D,#2a2a2a8D);fontFamily=Trebuchet MS,Verdana,Arial,sans-serif;fontColor=light-dark(#333333,#cccccc);exitX=1;exitY=0.51;entryX=0.06;entryY=1;" target="yTP9tqG_ZDtt-V5R-cQw-5">
            <mxGeometry relative="1" as="geometry">
              <Array as="points">
                <mxPoint x="877" y="164" />
              </Array>
            </mxGeometry>
          </mxCell>
        </UserObject>
        <UserObject label="" mermaidId="e:E-&gt;H#0" mermaidBaseStyle="curved=1;startArrow=none;endArrow=block;endSize=7;strokeColor=light-dark(#333333,#cccccc);exitX=1;exitY=0.51;entryX=0;entryY=0.51;" mermaidBaseValue="" id="yTP9tqG_ZDtt-V5R-cQw-17">
          <mxCell edge="1" parent="leXPLPGI7J2cDm4CVkzy-0" source="yTP9tqG_ZDtt-V5R-cQw-6" style="curved=1;startArrow=none;endArrow=block;endSize=7;strokeColor=light-dark(#333333,#cccccc);exitX=1;exitY=0.51;entryX=0;entryY=0.51;" target="yTP9tqG_ZDtt-V5R-cQw-9">
            <mxGeometry relative="1" as="geometry">
              <Array as="points" />
            </mxGeometry>
          </mxCell>
        </UserObject>
        <UserObject label="" mermaidId="e:H-&gt;I#0" mermaidBaseStyle="curved=1;startArrow=none;endArrow=block;endSize=7;strokeColor=light-dark(#333333,#cccccc);exitX=1;exitY=0.51;entryX=0;entryY=0.51;" mermaidBaseValue="" id="yTP9tqG_ZDtt-V5R-cQw-18">
          <mxCell edge="1" parent="leXPLPGI7J2cDm4CVkzy-0" source="yTP9tqG_ZDtt-V5R-cQw-9" style="curved=1;startArrow=none;endArrow=block;endSize=7;strokeColor=light-dark(#333333,#cccccc);exitX=1;exitY=0.51;entryX=0;entryY=0.51;" target="yTP9tqG_ZDtt-V5R-cQw-10">
            <mxGeometry relative="1" as="geometry">
              <Array as="points" />
            </mxGeometry>
          </mxCell>
        </UserObject>
      </root>
    </mxGraphModel>
  </diagram>
</mxfile>

```

| Service Docker | Rôle | URL |
|---|---|---|
| `kafka` (KRaft) | Broker, topic `toots` | `localhost:9092` |
| `kafka-ui` | Visualiser le topic (démo) | http://localhost:8080 |
| `postgres` | Stockage toots + agrégats | `localhost:5432` (spark/spark) |
| `producer` | Mastodon → Kafka | — |
| `jupyter` | Spark 3.5 + notebooks | http://localhost:8888 (token `spark`) |
| Spark UI | Suivi des jobs | http://localhost:4040 |

## Arborescence

```
├── docker-compose.yml
├── run.ps1 / Makefile           # commandes (Windows / bash)
├── .env.example
├── docker/jupyter/              # image Jupyter + PySpark + psycopg2
├── producer/                    # Mastodon -> Kafka (stream ou poll)
├── common/config.py             # config + SparkSession (connecteurs Kafka/JDBC)
├── common/text.py               # nettoyage texte commun train / inférence
├── ml/train_sentiment.py        # entraînement MLlib -> ml/models/
├── streaming/stream_processor.py
├── batch/batch_analysis.py
├── sql/init.sql                 # tables PostgreSQL
├── scripts/download_sentiment140.py
├── notebooks/01_exploration · 02_sentiment_model · 03_visualisations
└── outputs/                     # CSV + figures PNG pour les slides
```

## Prérequis

- **Docker Desktop** (WSL2 activé) avec ≥ 8 Go de RAM alloués (*Settings → Resources*)
- Un compte Mastodon et un **access token** : *Préférences → Développement → Nouvelle application*, scope `read`, puis copier « Votre jeton d'accès »

## Démarrage (Windows PowerShell)

```powershell
.\run.ps1 up          # 1er lancement : crée .env -> y coller MASTODON_TOKEN, puis relancer
.\run.ps1 up          # build + démarrage de tous les services
.\run.ps1 logs        # vérifier que le producer envoie des toots (Ctrl+C pour quitter)
.\run.ps1 consume     # voir 5 messages bruts dans Kafka

.\run.ps1 data        # télécharger Sentiment140 (~80 Mo)
.\run.ps1 train       # entraîner le modèle (LR + comparaison Naive Bayes)

.\run.ps1 stream      # streaming Kafka -> Spark -> PostgreSQL (laisser tourner, terminal dédié)
.\run.ps1 count       # dans un autre terminal : toots en base par sentiment

.\run.ps1 batch       # après quelques heures de collecte
```

Puis ouvrir http://localhost:8888 (token `spark`) → `notebooks/03_visualisations.ipynb` → *Run All*.

> Bloqué par la politique d'exécution ? `powershell -ExecutionPolicy Bypass -File .\run.ps1 up`
> Git Bash / WSL : `make up`, `make train`, `make stream`…

## Configuration (`.env`)

| Variable | Défaut | Rôle |
|---|---|---|
| `MASTODON_URL` / `MASTODON_TOKEN` | mastodon.social | Instance et jeton |
| `INGEST_MODE` | `stream` | `stream` temps réel, `poll` requêtes toutes les `POLL_INTERVAL` s. Bascule auto en poll si le stream échoue |
| `SOURCE` / `HASHTAGS` | `public` | `hashtag` pour suivre une liste de hashtags (plus de volume ciblé) |
| `FILTER_LANG` | `en` | Langue scorée (modèle entraîné en anglais) |
| `TRAIN_SAMPLE_FRACTION` | `0.3` | Part de Sentiment140 utilisée (1.0 = 1,6 M lignes, ~10 min) |
| `SPARK_DRIVER_MEMORY` | `2g` | Mémoire du driver Spark |

## Modèle de données

**Message Kafka** : `id, created_at (UTC), content (HTML), language, account, followers_count, reblogs_count, favourites_count, replies_count, hashtags[], url`

| Table | Écrite par | Contenu |
|---|---|---|
| `toots` | streaming | toot nettoyé + `sentiment` + `score_pos` (dédoublonné par `id`) |
| `sentiment_window` | streaming | nb de toots par sentiment, fenêtres 5 min (watermark 10 min) |
| `hashtag_window` | streaming | occurrences de hashtags, fenêtres 5 min |
| `batch_*` | batch | sentiment global, horaire, top hashtags, comptes, engagement, heatmap, corrélation |

## Choix techniques

- **Nettoyage unique** (`common/text.py`) utilisé à l'entraînement et en streaming → pas de décalage train/inférence.
- **Idempotence** : chaque micro-batch passe par `toots_staging` puis `INSERT … ON CONFLICT DO NOTHING` → pas de doublon en cas de redémarrage.
- **Checkpoints** Spark dans un volume Docker → reprise exacte après arrêt.
- **Watermark** 10 min + fenêtres 5 min en mode `append` : une fenêtre n'est écrite qu'une fois close (~15 min de délai).
- **Connecteur Kafka** résolu automatiquement selon la version de PySpark.

## Dépannage

| Symptôme | Solution |
|---|---|
| `producer` redémarre en boucle | Vérifier `MASTODON_TOKEN` dans `.env` puis `docker compose up -d producer` |
| Peu de toots | `SOURCE=hashtag` avec des hashtags actifs, ou `INGEST_MODE=poll` |
| `sentiment_window` vide | Normal les 15 premières minutes (fenêtre + watermark) |
| Erreur mémoire Spark | Baisser `TRAIN_SAMPLE_FRACTION` ou augmenter la RAM Docker |
| Image Jupyter introuvable | `docker compose build --build-arg BASE_IMAGE=quay.io/jupyter/pyspark-notebook:spark-3.5.3 jupyter` |
| Tout réinitialiser | `.\run.ps1 reset` (supprime les données) |

## Résultats

> À compléter : période et volume collectés, accuracy/F1/AUC du modèle, 3 à 5 insights.

## Limites et pistes

- Modèle entraîné sur des tweets 2009 (biais de domaine vs toots actuels)
- Sentiment binaire sans classe neutre ; toots non anglais ignorés → modèle multilingue / transformer
- Déploiement mono-nœud → cluster Spark et Kafka multi-brokers

## Auteur

Mohamed — Master Informatique, spécialisation Data & IA — SUPINFO Paris
