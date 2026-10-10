# Image Jupyter + Spark 3.5 (Scala 2.12). Tag surchargeable : docker compose build --build-arg BASE_IMAGE=...
ARG BASE_IMAGE=quay.io/jupyter/pyspark-notebook:spark-3.5.0
FROM ${BASE_IMAGE}

USER root
# Dossiers montés en volumes nommés : on les crée avec le bon propriétaire
RUN mkdir -p /home/jovyan/.ivy2 /home/jovyan/checkpoints && \
    chown -R ${NB_UID}:${NB_GID} /home/jovyan/.ivy2 /home/jovyan/checkpoints
USER ${NB_UID}

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
