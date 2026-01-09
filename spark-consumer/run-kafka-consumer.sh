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
export KAFKA_STARTING_OFFSETS="${KAFKA_STARTING_OFFSETS:-latest}"

export S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"
export S3_ACCESS_KEY="${S3_ACCESS_KEY:-admin}"
export S3_SECRET_KEY="${S3_SECRET_KEY:-password}"

# You can use local FS or S3A. Default to S3A (MinIO) for realistic checkpointing.
export CHECKPOINT_LOCATION="${CHECKPOINT_LOCATION:-s3a://datalake/checkpoints/kafka-consumer/$(date +%s)}"

# Run two console sinks from the same job: one for payload and one for headers.
export DUAL_STREAM="${DUAL_STREAM:-true}"

export PYTHONPATH="${REPO_ROOT}/generated/python:${SCRIPT_DIR}/src${PYTHONPATH:+:${PYTHONPATH}}"

cd "${SCRIPT_DIR}"

uv sync

mkdir -p "${REPO_ROOT}/proto/descriptors"
PATH="$HOME/.local/bin:$PATH" buf build --as-file-descriptor-set -o "${REPO_ROOT}/proto/descriptors/demo.desc" "${REPO_ROOT}/proto"

export USE_S3_DESCRIPTOR="${USE_S3_DESCRIPTOR:-true}"
export UPLOAD_DESCRIPTOR="${UPLOAD_DESCRIPTOR:-false}"
export DESCRIPTOR_BUCKET="${DESCRIPTOR_BUCKET:-proto-artifacts}"
export DESCRIPTOR_KEY="${DESCRIPTOR_KEY:-descriptors/demo.desc}"
export DESCRIPTOR_FILE="${DESCRIPTOR_FILE:-proto/descriptors/demo.desc}"

if [[ "${USE_S3_DESCRIPTOR}" == "true" || "${USE_S3_DESCRIPTOR}" == "1" || "${USE_S3_DESCRIPTOR}" == "yes" ]]; then
  export PROTO_DESCRIPTOR_PATH="${PROTO_DESCRIPTOR_PATH:-s3a://${DESCRIPTOR_BUCKET}/${DESCRIPTOR_KEY}}"

  if [[ "${UPLOAD_DESCRIPTOR}" == "true" || "${UPLOAD_DESCRIPTOR}" == "1" || "${UPLOAD_DESCRIPTOR}" == "yes" ]]; then
    (cd "${REPO_ROOT}" && ./gradlew --no-configuration-cache :s3-upload:uploadDescriptor)
  fi
fi

(cd "${REPO_ROOT}" && ./gradlew --no-configuration-cache :spark-udf:jar)

export PROTO_DESCRIPTOR_PATH="${PROTO_DESCRIPTOR_PATH:-${REPO_ROOT}/proto/descriptors/demo.desc}"
# If you set this, kafka_consumer.py will decode and print msg.id/msg.payload.
export PROTO_MESSAGE_NAME="${PROTO_MESSAGE_NAME:-acme.demo.v1.DemoEvent}"

export STRIP_UDF_CLASS="${STRIP_UDF_CLASS:-com.acme.spark.StripConfluentProtobufUdf}"
export STRIP_UDF_NAME="${STRIP_UDF_NAME:-strip_confluent_protobuf}"

export PYSPARK_SUBMIT_ARGS="${PYSPARK_SUBMIT_ARGS:-} --jars ${REPO_ROOT}/spark-udf/build/libs/spark-udf.jar pyspark-shell"

exec uv run python -m spark_consumer.kafka_consumer
