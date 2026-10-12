#!/usr/bin/env bash
# Удалить кластер целиком, вместе с томами (данные DWH и S3 в нём пропадут).
# Контейнеры compose остаются, их только отключают от сети kind.
set -euo pipefail
cd "$(dirname "$0")/../.."

for svc in oracle crm-db kafka clickhouse; do
  cid=$(docker compose ps -q "$svc" 2>/dev/null || true)
  [[ -n "$cid" ]] && docker network disconnect kind "$cid" 2>/dev/null || true
done
kind delete cluster --name retail-dwh
