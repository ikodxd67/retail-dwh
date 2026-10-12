#!/usr/bin/env bash
# Поднять стенд в kind с нуля (или обновить уже поднятый). Повторный
# запуск безопасен: каждый шаг идемпотентен.
#
#   k8s/scripts/up.sh                  всё
#   SKIP_BUILD=1 k8s/scripts/up.sh     без пересборки образа Airflow
set -euo pipefail

CLUSTER=retail-dwh
NS=retail-dwh
IMAGE=retail-dwh/airflow:k8s
CHART_VERSION=1.22.0
cd "$(dirname "$0")/../.."

step() { printf '\n== %s\n' "$*"; }

for tool in docker kind kubectl helm; do
  command -v "$tool" >/dev/null || { echo "не найден $tool" >&2; exit 1; }
done

step "кластер"
if ! kind get clusters | grep -qx "$CLUSTER"; then
  kind create cluster --config k8s/kind-config.yaml --wait 120s
fi
kubectl config use-context "kind-$CLUSTER" >/dev/null

if [[ -z "${SKIP_BUILD:-}" ]]; then
  step "образ Airflow с кодом проекта"
  docker build -f docker/airflow/Dockerfile --target k8s -t "$IMAGE" .
  # в kind нет доступа к локальным образам Docker: образ копируется
  # внутрь узла, и pullPolicy: Never запрещает искать его в реестре
  kind load docker-image "$IMAGE" --name "$CLUSTER"
fi

step "хранилища, Spark, Trino"
kubectl kustomize --load-restrictor LoadRestrictionsNone k8s/base | kubectl apply -f -
kubectl -n "$NS" rollout status statefulset/dwh-db --timeout=300s
kubectl -n "$NS" rollout status statefulset/s3 --timeout=300s

step "миграции и бакеты"
kubectl -n "$NS" delete job bootstrap --ignore-not-found
kubectl apply -f k8s/jobs/bootstrap.yaml
kubectl -n "$NS" wait --for=condition=complete job/bootstrap --timeout=300s
kubectl -n "$NS" logs job/bootstrap | tail -3

step "Airflow"
helm repo add apache-airflow https://airflow.apache.org >/dev/null 2>&1 || true
helm repo update apache-airflow >/dev/null
# Миграция Airflow — обычный Job (не хук, см. values.yaml), а spec у Job
# неизменяем: если шаблон поменялся, upgrade упадёт с «field is immutable».
# Старый Job удаляем, чарт создаст новый.
kubectl -n "$NS" delete job airflow-run-airflow-migrations --ignore-not-found
# если образ пересобран под тем же тегом, поды сами его не подхватят —
# аннотация с датой заставляет чарт пересоздать их
helm upgrade --install airflow apache-airflow/airflow \
  --version "$CHART_VERSION" \
  --namespace "$NS" \
  --values k8s/airflow/values.yaml \
  --set-string "airflowPodAnnotations.retail-dwh/deployed-at=$(date +%s)" \
  --wait --timeout 15m

step "сервисы из docker compose"
k8s/scripts/link-compose.sh

step "готово"
kubectl -n "$NS" get pods
cat <<'EOF'

Интерфейсы (каждый — в отдельном терминале):
  kubectl -n retail-dwh port-forward svc/airflow-api-server 8080:8080
  kubectl -n retail-dwh port-forward svc/spark 4040:4040
  kubectl -n retail-dwh port-forward svc/trino 8085:8080
EOF
