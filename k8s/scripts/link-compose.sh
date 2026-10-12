#!/usr/bin/env bash
# Делает сервисы из docker compose видимыми в кластере под теми же именами.
#
# Источники (Oracle, CRM), Kafka и ClickHouse в кластер не переезжают:
# в жизни это чужие системы, которые живут вне нашего Kubernetes. Чтобы
# поды находили их по привычным именам (oracle, kafka, ...), для каждого
# заводится Service без селектора и EndpointSlice с IP контейнера:
#
#   под -> Service "kafka" -> EndpointSlice -> контейнер kafka в compose
#
# Контейнер для этого подключается к docker-сети kind, где живёт узел
# кластера. IP после перезапуска контейнера может смениться — тогда
# скрипт нужно запустить ещё раз.
set -euo pipefail

NS=retail-dwh
NETWORK=kind
cd "$(dirname "$0")/../.."

# сервис compose -> порт, который нужен подам
declare -A PORTS=(
  [oracle]=1521
  [crm-db]=5432
  [kafka]=19092
  [clickhouse]=8123
)

for svc in "${!PORTS[@]}"; do
  cid=$(docker compose ps -q "$svc" 2>/dev/null || true)
  if [[ -z "$cid" ]]; then
    echo "пропускаю $svc: контейнер не запущен"
    continue
  fi
  if ! docker inspect -f '{{json .NetworkSettings.Networks}}' "$cid" | grep -q "\"$NETWORK\""; then
    docker network connect "$NETWORK" "$cid"
  fi
  ip=$(docker inspect -f "{{(index .NetworkSettings.Networks \"$NETWORK\").IPAddress}}" "$cid")
  port=${PORTS[$svc]}
  echo "$svc -> $ip:$port"

  kubectl apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: $svc
  namespace: $NS
  labels:
    app.kubernetes.io/part-of: retail-dwh
    retail-dwh/external: compose
spec:
  ports:
    - name: main
      port: $port
      targetPort: $port
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: $svc-compose
  namespace: $NS
  labels:
    kubernetes.io/service-name: $svc
    retail-dwh/external: compose
addressType: IPv4
ports:
  - name: main
    port: $port
    protocol: TCP
endpoints:
  - addresses: ["$ip"]
EOF
done
