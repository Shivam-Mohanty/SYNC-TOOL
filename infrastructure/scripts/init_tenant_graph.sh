#!/usr/bin/env bash
# init_tenant_graph.sh
set -euo pipefail

ORG_ID=${1:-""}
if [ -z "$ORG_ID" ]; then
    echo "Usage: ./init_tenant_graph.sh <org_id>"
    exit 1
fi

# Sanitize org_id: remove hyphens and special characters
CLEAN_ORG_ID=$(echo "$ORG_ID" | tr -d '-' | tr '[:upper:]' '[:lower:]')

DB_HOST=${POSTGRES_HOST:-"localhost"}
DB_PORT=${POSTGRES_PORT:-"5432"}
DB_USER=${POSTGRES_USER:-"postgres"}
DB_NAME=${POSTGRES_DB:-"sync_tool"}

echo "Initializing AGE graph 'workspace_graph_${CLEAN_ORG_ID}' on ${DB_HOST}:${DB_PORT}/${DB_NAME}..."

PGPASSWORD=${POSTGRES_PASSWORD:-"postgres"} psql \
    -h "$DB_HOST" \
    -p "$DB_PORT" \
    -U "$DB_USER" \
    -d "$DB_NAME" \
    -v org_id="$CLEAN_ORG_ID" \
    -f "$(dirname "$0")/init_tenant_graph.sql"

echo "Graph 'workspace_graph_${CLEAN_ORG_ID}' initialized successfully."
