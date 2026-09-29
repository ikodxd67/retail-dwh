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
    "sqlglot>=25.0"
