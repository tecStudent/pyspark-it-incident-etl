FROM spark:python3@sha256:469921a4c3dbb534b17fa51ff4da61fa1fda3c27f0882fa7809b63c2c2bc2d8d

ARG VERSION=dev
ARG REVISION=unknown

LABEL org.opencontainers.image.title="PySpark IT Incident ETL" \
      org.opencontainers.image.description="Pipeline de incidentes de TI com PySpark" \
      org.opencontainers.image.source="https://github.com/tecStudent/pyspark-it-incident-etl" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"

USER root

COPY requirements.txt /tmp/requirements.txt

RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app

COPY --chown=185:0 src/ /app/src/
COPY --chown=185:0 VERSION /app/VERSION
COPY --chown=185:0 data/sample/ /app/data/sample/

ENV PYTHONPATH="/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip"

USER 185

CMD ["python3", "src/incremental_pipeline.py"]
