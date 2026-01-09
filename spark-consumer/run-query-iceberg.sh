#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

JAVA_17_HOME_DEFAULT="$HOME/.sdkman/candidates/java/17.0.13-tem"
if [[ -n "${JAVA_HOME_OVERRIDE:-}" ]]; then
  export JAVA_HOME="$JAVA_HOME_OVERRIDE"
elif [[ -d "$JAVA_17_HOME_DEFAULT" ]]; then
  export JAVA_HOME="$JAVA_17_HOME_DEFAULT"
fi
export PATH="$JAVA_HOME/bin:$PATH"

echo "JAVA_HOME=$JAVA_HOME"
java -version

export PYTHONPATH="${REPO_ROOT}/generated/python:${SCRIPT_DIR}/src${PYTHONPATH:+:${PYTHONPATH}}"

# Iceberg REST catalog (Glue-like) + MinIO defaults
export ICEBERG_CATALOG_URI="${ICEBERG_CATALOG_URI:-http://localhost:8181}"
export ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE:-s3://datalake/warehouse}"
export ICEBERG_CATALOG="${ICEBERG_CATALOG:-iceberg}"
export ICEBERG_NAMESPACE="${ICEBERG_NAMESPACE:-demo}"
export ICEBERG_TABLE="${ICEBERG_TABLE:-demo_events}"
export ICEBERG_QUERY_LIMIT="${ICEBERG_QUERY_LIMIT:-50}"

export S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"
export S3_ACCESS_KEY="${S3_ACCESS_KEY:-admin}"
export S3_SECRET_KEY="${S3_SECRET_KEY:-password}"

# Iceberg S3FileIO uses AWS SDK credentials/region resolution.
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-$S3_ACCESS_KEY}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-$S3_SECRET_KEY}"

cd "${SCRIPT_DIR}"

uv sync

exec uv run python -m spark_consumer.query_iceberg
