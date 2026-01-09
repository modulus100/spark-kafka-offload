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

export CHECKPOINT_LOCATION="${CHECKPOINT_LOCATION:-/tmp/spark-kafka-consumer-checkpoint-$(date +%s)}"

# Run two console sinks from the same job: one for payload and one for headers.
export DUAL_STREAM="${DUAL_STREAM:-true}"

export PYTHONPATH="${REPO_ROOT}/generated/python:${SCRIPT_DIR}/src${PYTHONPATH:+:${PYTHONPATH}}"

cd "${SCRIPT_DIR}"

uv sync

mkdir -p "${REPO_ROOT}/proto/descriptors"
PATH="$HOME/.local/bin:$PATH" buf build --as-file-descriptor-set -o "${REPO_ROOT}/proto/descriptors/demo.desc" "${REPO_ROOT}/proto"

(cd "${REPO_ROOT}" && ./gradlew --no-configuration-cache :spark-udf:jar)

export PROTO_DESCRIPTOR_PATH="${PROTO_DESCRIPTOR_PATH:-${REPO_ROOT}/proto/descriptors/demo.desc}"
export PROTO_MESSAGE_NAME="${PROTO_MESSAGE_NAME:-acme.demo.v1.DemoEvent}"

export STRIP_UDF_CLASS="${STRIP_UDF_CLASS:-com.acme.spark.StripConfluentProtobufUdf}"
export STRIP_UDF_NAME="${STRIP_UDF_NAME:-strip_confluent_protobuf}"

export PYSPARK_SUBMIT_ARGS="${PYSPARK_SUBMIT_ARGS:-} --jars ${REPO_ROOT}/spark-udf/build/libs/spark-udf.jar pyspark-shell"

exec uv run python -m spark_consumer.kafka_consumer
