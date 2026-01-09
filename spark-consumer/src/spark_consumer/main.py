from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr
from pyspark.sql.types import BinaryType
from pyspark.sql.protobuf.functions import from_protobuf


def _write_to_iceberg(batch_df, batch_id: int) -> None:
    spark = batch_df.sparkSession
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.demo")

    (
        batch_df.writeTo("iceberg.demo.demo_events")
        .append()
    )


def main() -> None:
    topic = os.environ.get("KAFKA_TOPIC", "demo.protobuf")
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    descriptor_path = os.environ.get("PROTO_DESCRIPTOR_PATH", "../proto/descriptors/demo.desc")
    message_name = os.environ.get("PROTO_MESSAGE_NAME", "acme.demo.v1.DemoEvent")
    udf_name = os.environ.get("STRIP_UDF_NAME", "strip_confluent_protobuf")
    udf_class = os.environ.get(
        "STRIP_UDF_CLASS",
        "com.acme.spark.StripConfluentProtobufUdf",
    )

    spark = (
        SparkSession.builder.appName("spark-consumer")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,"
            "org.apache.spark:spark-protobuf_2.12:3.5.2,"
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
            "org.apache.hadoop:hadoop-aws:3.3.4",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", os.environ.get("ICEBERG_CATALOG_URI", "http://localhost:8181"))
        .config("spark.sql.catalog.iceberg.warehouse", os.environ.get("ICEBERG_WAREHOUSE", "s3a://datalake/warehouse"))
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("S3_ENDPOINT", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("S3_ACCESS_KEY", "admin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("S3_SECRET_KEY", "password"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    # Register JVM UDF to strip Confluent Protobuf framing -> raw protobuf payload.
    spark.udf.registerJavaFunction(udf_name, udf_class, BinaryType())

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    decoded = df.select(
        from_protobuf(expr(f"{udf_name}(value)"), message_name, descriptor_path).alias("msg")
    ).select(
        col("msg.id").alias("id"),
        col("msg.payload").alias("payload"),
        current_timestamp().alias("ingested_at"),
    )

    query = (
        decoded.writeStream.foreachBatch(_write_to_iceberg)
        .option("checkpointLocation", os.environ.get("CHECKPOINT_LOCATION", "/tmp/spark-consumer-checkpoint"))
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
