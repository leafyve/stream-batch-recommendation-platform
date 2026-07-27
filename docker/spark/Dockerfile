FROM apache/spark:3.5.8-scala2.12-java17-python3-ubuntu@sha256:dbe5f3b3926b01fa0b818ae8463c2994bfd2945d8db96d2337e23dc62d6fa73d

USER root
WORKDIR /opt/app

COPY docker/spark/fetch_jars.py /tmp/fetch_jars.py
RUN python3 /tmp/fetch_jars.py /opt/spark/jars \
    && rm /tmp/fetch_jars.py

RUN python3 -m pip install --no-cache-dir \
    "psutil==7.0.0" \
    "pytest==8.4.1" \
    "PyYAML==6.0.2" \
    "structlog==25.4.0"

COPY src /opt/app/src
COPY tests/spark /opt/app/tests/spark
COPY pyproject.toml /opt/app/
RUN mkdir -p /opt/app/jobs \
    && cp /opt/app/src/recommendation_features/iceberg_tables.py /opt/app/jobs/ \
    && cp /opt/app/src/recommendation_features/streaming_job.py /opt/app/jobs/ \
    && cp /opt/app/src/recommendation_features/batch_job.py /opt/app/jobs/ \
    && cp /opt/app/src/recommendation_features/parity.py /opt/app/jobs/ \
    && cp /opt/app/src/recommendation_features/platform_probe.py /opt/app/jobs/

COPY configs/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY configs/log4j2.properties /opt/spark/conf/log4j2.properties

ENV PYTHONPATH=/opt/app/src:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
USER spark
