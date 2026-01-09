from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.sql.types import BinaryType


def main() -> None:
    topic = os.environ.get("KAFKA_TOPIC", "demo.protobuf")
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    print_mode = os.environ.get("PRINT_MODE", "payload").strip().lower()
    dual_stream = os.environ.get("DUAL_STREAM", "false").strip().lower() in ("1", "true", "yes")

    # If PROTO_MESSAGE_NAME is set, we attempt to decode with from_protobuf.
    # If it's empty/unset, we just print the raw bytes as hex.
    descriptor_path = os.environ.get("PROTO_DESCRIPTOR_PATH", "../proto/descriptors/demo.desc")
    message_name = os.environ.get("PROTO_MESSAGE_NAME", "")

    udf_name = os.environ.get("STRIP_UDF_NAME", "strip_confluent_protobuf")
    udf_class = os.environ.get(
        "STRIP_UDF_CLASS",
        "com.acme.spark.StripConfluentProtobufUdf",
    )

    s3_endpoint = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
    s3_access_key = os.environ.get("S3_ACCESS_KEY", "admin")
    s3_secret_key = os.environ.get("S3_SECRET_KEY", "password")

    spark = (
        SparkSession.builder.appName("kafka-consumer-debug")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,"
            "org.apache.spark:spark-protobuf_2.12:3.5.2,"
            "org.apache.hadoop:hadoop-aws:3.3.4",
        )
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    spark.udf.registerJavaFunction(udf_name, udf_class, BinaryType())

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", os.environ.get("KAFKA_STARTING_OFFSETS", "latest"))
        .option("includeHeaders", "true")
        .load()
    )

    # Spark Kafka source exposes headers as: array<struct<key:string,value:binary>>
    headers_map = expr(
        "map_from_entries(transform(headers, h -> struct(h.key, cast(h.value as string))))"
    )

    base = df.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("key").cast("string").alias("key"),
        col("value").alias("value"),
        headers_map.alias("headers"),
    )

    if message_name:
        decoded = base.select(
            "topic",
            "partition",
            "offset",
            "timestamp",
            "key",
            col("headers").getItem("ce_id").alias("ce_id"),
            col("headers").getItem("ce_specversion").alias("ce_specversion"),
            col("headers").getItem("ce_type").alias("ce_type"),
            col("headers").getItem("ce_source").alias("ce_source"),
            from_protobuf(expr(f"{udf_name}(value)"), message_name, descriptor_path).alias("msg"),
        )
        payload_out = decoded.select(
            "topic",
            "partition",
            "offset",
            "timestamp",
            "key",
            col("msg.id").alias("id"),
            col("msg.payload").alias("payload"),
        )
        headers_out = decoded.select(
            "topic",
            "partition",
            "offset",
            "timestamp",
            "key",
            "ce_id",
            "ce_specversion",
            "ce_type",
            "ce_source",
        )
    else:
        payload_out = base.select(
            "topic",
            "partition",
            "offset",
            "timestamp",
            "key",
            expr("hex(value)").alias("value_hex"),
        )
        headers_out = base.select(
            "topic",
            "partition",
            "offset",
            "timestamp",
            "key",
            col("headers").getItem("ce_id").alias("ce_id"),
            col("headers").getItem("ce_specversion").alias("ce_specversion"),
            col("headers").getItem("ce_type").alias("ce_type"),
            col("headers").getItem("ce_source").alias("ce_source"),
        )

    checkpoint_base = os.environ.get("CHECKPOINT_LOCATION", "/tmp/spark-kafka-consumer-checkpoint")

    if dual_stream:
        q_payload = (
            payload_out.writeStream.format("console")
            .queryName("kafka_consumer_payload")
            .outputMode("append")
            .option("truncate", "false")
            .option("checkpointLocation", f"{checkpoint_base}-payload")
            .start()
        )

        q_headers = (
            headers_out.writeStream.format("console")
            .queryName("kafka_consumer_headers")
            .outputMode("append")
            .option("truncate", "false")
            .option("checkpointLocation", f"{checkpoint_base}-headers")
            .start()
        )

        # Keep both running
        spark.streams.awaitAnyTermination()
        q_payload.stop()
        q_headers.stop()
    else:
        out = headers_out if print_mode == "headers" else payload_out

        query = (
            out.writeStream.format("console")
            .outputMode("append")
            .option("truncate", "false")
            .option("checkpointLocation", checkpoint_base)
            .start()
        )

        query.awaitTermination()


if __name__ == "__main__":
    main()
