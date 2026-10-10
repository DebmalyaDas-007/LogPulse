#!/usr/bin/env sh
set -eu
[ -f .env ] && set -a && . ./.env && set +a

es="${ELASTICSEARCH_URL:-http://localhost:9200}"
for name in server-logs anomalies; do
  curl --fail-with-body -sS -X PUT "$es/_index_template/$name-template" \
    -H 'Content-Type: application/json' \
    --data-binary "@elastic_config/index_templates/$name-template.json"
  echo
done
