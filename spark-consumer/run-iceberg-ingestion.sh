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

export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-demo.protobuf}"

export CHECKPOINT_LOCATION="${CHECKPOINT_LOCATION:-/tmp/spark-iceberg-ingestion-checkpoint-$(date +%s)}"

export PYTHONPATH="${REPO_ROOT}/generated/python:${SCRIPT_DIR}/src${PYTHONPATH:+:${PYTHONPATH}}"

# Iceberg REST catalog (Glue-like) + MinIO S3A defaults
export ICEBERG_CATALOG_URI="${ICEBERG_CATALOG_URI:-http://localhost:8181}"
export ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE:-s3://datalake/warehouse}"
export ICEBERG_NAMESPACE="${ICEBERG_NAMESPACE:-demo}"
export ICEBERG_TABLE="${ICEBERG_TABLE:-demo_events}"

export S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"
export S3_ACCESS_KEY="${S3_ACCESS_KEY:-admin}"
export S3_SECRET_KEY="${S3_SECRET_KEY:-password}"

# Iceberg S3FileIO uses AWS SDK credentials/region resolution.
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-$S3_ACCESS_KEY}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-$S3_SECRET_KEY}"

cd "${SCRIPT_DIR}"

uv sync

mkdir -p "${REPO_ROOT}/proto/descriptors"
PATH="$HOME/.local/bin:$PATH" buf build --as-file-descriptor-set -o "${REPO_ROOT}/proto/descriptors/demo.desc" "${REPO_ROOT}/proto"

(cd "${REPO_ROOT}" && ./gradlew --no-configuration-cache :spark-udf:jar)

export PROTO_DESCRIPTOR_PATH="${PROTO_DESCRIPTOR_PATH:-${REPO_ROOT}/proto/descriptors/demo.desc}"
export STRIP_UDF_CLASS="${STRIP_UDF_CLASS:-com.acme.spark.StripConfluentProtobufUdf}"
export STRIP_UDF_NAME="${STRIP_UDF_NAME:-strip_confluent_protobuf}"

# Make the UDF jar available to Spark JVM
export PYSPARK_SUBMIT_ARGS="${PYSPARK_SUBMIT_ARGS:-} --jars ${REPO_ROOT}/spark-udf/build/libs/spark-udf.jar pyspark-shell"

exec uv run python -m spark_consumer.kafka_to_iceberg
