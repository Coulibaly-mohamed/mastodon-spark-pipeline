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

# Rend le dossier du projet importable (from common import config) sans toucher au PYTHONPATH de l'image
RUN echo "/home/jovyan/work" > "$(python -c 'import site; print(site.getsitepackages()[0])')/mastodon_project.pth"

# Installe pyspark (même version que le Spark de l'image) dans site-packages :
# "import pyspark" fonctionne ainsi partout (docker compose exec, scripts, notebooks), quel que soit le PYTHONPATH
RUN SPARK_VER=$(ls ${SPARK_HOME:-/usr/local/spark}/jars/spark-core_*.jar | sed -E 's/.*spark-core_[0-9.]+-([0-9.]+)\.jar/\1/') && \
    echo "Spark de l'image : ${SPARK_VER}" && \
    pip install --no-cache-dir "pyspark==${SPARK_VER}"
RUN env -u PYTHONPATH python -c "import pyspark; print('pyspark', pyspark.__version__, 'OK')"
