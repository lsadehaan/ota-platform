#!/bin/sh
set -eu

project="${COMPOSE_PROJECT_NAME:-deployments}"
out="${PROMETHEUS_OUT:-/out/prometheus.yml}"
retries="${PROMETHEUS_CONFIG_RETRIES:-30}"
sleep_secs="${PROMETHEUS_CONFIG_SLEEP_SECS:-2}"

wait_for_container() {
  pattern="$1"
  i=0
  while [ "$i" -lt "$retries" ]; do
    if docker ps --format '{{.Names}}' | grep -Eq "$pattern"; then
      return 0
    fi
    i=$((i+1))
    sleep "$sleep_secs"
  done
  return 1
}

wait_for_container "^${project}-ota-engine-[0-9]+$"

mkdir -p "$(dirname "$out")"
cat > "$out" <<CFG
global:
  scrape_interval: 5s
  evaluation_interval: 5s

scrape_configs:
  - job_name: otel-collector
    static_configs:
      - targets: ['otel-collector:9464']

  - job_name: postgres-exporter
    static_configs:
      - targets: ['postgres-exporter:9187']

  - job_name: node-exporter
    static_configs:
      - targets: ['node-exporter:9100']

  - job_name: cadvisor
    static_configs:
      - targets: ['cadvisor:8080']
    metric_relabel_configs:
CFG

docker ps --format '{{.ID}} {{.Names}}' | while read -r id name; do
  case "$name" in
    ${project}-*)
      service=$(printf '%s' "$name" | sed "s/^${project}-//" | sed 's/-[0-9]*$//')
      case "$service" in
        ota-engine|mock-smsc|web|postgres)
          cat >> "$out" <<CFG
      - source_labels: [id]
        regex: .*/docker-${id}.*\\.scope
        target_label: service
        replacement: ${service}
CFG
          ;;
      esac
      ;;
  esac
done

echo "prometheus config written to $out"
