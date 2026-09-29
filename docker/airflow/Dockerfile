# Airflow с зависимостями проекта. Сам код не копируется в образ, а
# монтируется томом (см. docker-compose.yml): правка видна без пересборки.
FROM apache/airflow:3.3.2-python3.12

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
