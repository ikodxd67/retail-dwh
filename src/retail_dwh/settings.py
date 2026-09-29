"""Настройки из переменных окружения RETAIL_*.

С хоста значения берутся из .env в корне проекта, внутри compose их задаёт
docker-compose.yml. Переменные окружения всегда важнее файла.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

PROJECT_DIR = Path(os.environ.get("RETAIL_PROJECT_DIR", Path(__file__).resolve().parents[2]))


def _read_dotenv(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


@dataclass(frozen=True)
class Settings:
    oracle_dsn: str
    oracle_user: str
    oracle_password: str
    crm_dsn: str
    dwh_dsn: str
    dwh_readonly_dsn: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    clickhouse_host: str
    clickhouse_port: int
    kafka_bootstrap: str
    pushgateway: str
    trino_host: str
    trino_port: int
    spark_connect: str

    @classmethod
    def from_env(cls) -> Settings:
        env = _read_dotenv(PROJECT_DIR / ".env") | dict(os.environ)

        def get(name: str, default: str) -> str:
            return env.get(f"RETAIL_{name}", default)

        return cls(
            oracle_dsn=get("ORACLE_DSN", "localhost:1521/FREEPDB1"),
            oracle_user=get("ORACLE_USER", "erp"),
            oracle_password=get("ORACLE_PASSWORD", "erp"),
            crm_dsn=get("CRM_DSN", "postgresql://crm:crm@localhost:5433/crm"),
            dwh_dsn=get("DWH_DSN", "postgresql://dwh:dwh@localhost:5432/dwh"),
            dwh_readonly_dsn=get(
                "DWH_READONLY_DSN", "postgresql://monitoring_ro:monitoring_ro@localhost:5432/dwh"
            ),
            s3_endpoint=get("S3_ENDPOINT", "http://localhost:9000"),
            s3_access_key=get("S3_ACCESS_KEY", "minio"),
            s3_secret_key=get("S3_SECRET_KEY", "minio12345"),
            clickhouse_host=get("CLICKHOUSE_HOST", "localhost"),
            clickhouse_port=int(get("CLICKHOUSE_PORT", "8123")),
            kafka_bootstrap=get("KAFKA_BOOTSTRAP", "localhost:9092"),
            pushgateway=get("PUSHGATEWAY", ""),
            trino_host=get("TRINO_HOST", "localhost"),
            trino_port=int(get("TRINO_PORT", "8085")),
            spark_connect=get("SPARK_CONNECT", "sc://localhost:15002"),
        )


@lru_cache
def settings() -> Settings:
    return Settings.from_env()
