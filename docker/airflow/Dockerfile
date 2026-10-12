# Airflow с зависимостями проекта. Две цели сборки:
#   base — для docker compose: код не копируется, а монтируется томом,
#          правка видна без пересборки;
#   k8s  — для Kubernetes: код и DAG-и лежат внутри образа. При
#          KubernetesExecutor каждая задача стартует в новом поде, и
#          монтировать туда папку с ноутбука некуда.
FROM apache/airflow:3.3.2-python3.12 AS base

USER airflow

# Версии не закреплены жёстко: Airflow ставится со своим constraints-файлом,
# и жёсткие пины здесь дрались бы с ним.
RUN pip install --no-cache-dir \
    "psycopg[binary]>=3.2" \
    "oracledb>=2.4" \
    "clickhouse-connect>=0.8" \
    "defusedxml>=0.7" \
    "prometheus-client>=0.20" \
    "sqlglot>=25.0"     "trino>=0.330"     "pyspark[connect]==3.5.6"     setuptools
# pyspark[connect] — только клиент Spark Connect: без JVM, планы уходят на
# сервер Spark по gRPC. Версия клиента обязана совпадать с сервером (3.5.6).
# setuptools нужен pyspark 3.5 на Python 3.12: тот импортирует distutils,
# которого в 3.12 уже нет, а setuptools подкладывает его замену.


FROM base AS k8s

ENV PYTHONPATH=/opt/retail/src \
    RETAIL_PROJECT_DIR=/opt/retail

COPY --chown=airflow:root src /opt/retail/src
COPY --chown=airflow:root pipelines /opt/retail/pipelines
COPY --chown=airflow:root models /opt/retail/models
COPY --chown=airflow:root sql /opt/retail/sql
COPY --chown=airflow:root catalog /opt/retail/catalog
COPY --chown=airflow:root spark /opt/retail/spark
COPY --chown=airflow:root dags /opt/airflow/dags
