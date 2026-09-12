#!/bin/sh
# Inject SLACK_WEBHOOK env var into alertmanager.yml at container startup
# Uses sed to replace the SLACK_WEBHOOK_PLACEHOLDER token with the real URL.

set -e

CONFIG_SRC="/etc/alertmanager/alertmanager.yml"
CONFIG_RUNTIME="/tmp/alertmanager.yml"

WEBHOOK="${SLACK_WEBHOOK:-https://hooks.slack.com/services/PLACEHOLDER/PLACEHOLDER/PLACEHOLDER}"

sed "s|SLACK_WEBHOOK_PLACEHOLDER|${WEBHOOK}|g" "$CONFIG_SRC" > "$CONFIG_RUNTIME"

exec /bin/alertmanager \
  --config.file="$CONFIG_RUNTIME" \
  --storage.path=/alertmanager \
  --web.external-url="${ALERTMANAGER_EXTERNAL_URL:-http://localhost:9093}"
