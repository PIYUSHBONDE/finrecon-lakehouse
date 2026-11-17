FROM apache/spark:3.5.7-python3 AS spark_distribution

FROM python:3.11-slim-bookworm
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*
COPY --from=spark_distribution /opt/spark /opt/spark
# PySpark itself is supplied by the Apache distribution copied above.
RUN pip install --no-cache-dir --no-deps delta-spark==3.3.2 importlib-metadata==8.9.0 zipp==4.1.2 PyYAML==6.0.3 \
    && useradd --system --uid 185 --gid 0 --create-home --shell /bin/bash spark \
    && mkdir -p /lakehouse /opt/spark-ivy \
    && chown -R 185:0 /lakehouse /opt/spark-ivy

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV HOME=/home/spark
ENV PATH=/opt/spark/bin:$PATH
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/workspace
WORKDIR /workspace
USER 185
ENTRYPOINT []
CMD ["python3", "-m", "pipelines.bronze"]
