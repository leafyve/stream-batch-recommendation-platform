FROM python:3.11.9-slim-bookworm@sha256:8fb099199b9f2d70342674bd9dbccd3ed03a258f26bbd1d556822c6dfc60c317

RUN groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --create-home app

WORKDIR /opt/app
COPY pyproject.toml README.md /opt/app/
COPY src /opt/app/src
COPY configs /opt/app/configs
RUN python -m pip install --no-cache-dir \
      "confluent-kafka==2.15.0" \
      "psutil==7.0.0" \
      "PyYAML==6.0.2" \
      "structlog==25.4.0" \
    && python -m pip install --no-cache-dir --no-deps /opt/app

ENV PYTHONPATH=/opt/app/src
USER app
ENTRYPOINT ["python", "-m", "recommendation_features.producer"]
