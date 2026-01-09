from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr
from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.sql.types import BinaryType


def _write_to_iceberg(batch_df, batch_id: int) -> None:
    spark = batch_df.sparkSession

    catalog = os.environ.get("ICEBERG_CATALOG", "iceberg")
    namespace = os.environ.get("ICEBERG_NAMESPACE", "demo")
    table = os.environ.get("ICEBERG_TABLE", "demo_events")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.{table} (
          id STRING,
          payload STRING,
          ingested_at TIMESTAMP
        )
        USING iceberg
        """.strip()
    )

    batch_df.writeTo(f"{catalog}.{namespace}.{table}").append()


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

    iceberg_catalog_uri = os.environ.get("ICEBERG_CATALOG_URI", "http://localhost:8181")
    iceberg_warehouse = os.environ.get("ICEBERG_WAREHOUSE", "s3://datalake/warehouse")

    s3_endpoint = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
    s3_access_key = os.environ.get("S3_ACCESS_KEY", "admin")
    s3_secret_key = os.environ.get("S3_SECRET_KEY", "password")
    aws_region = os.environ.get("AWS_REGION", "us-east-1")

    spark = (
        SparkSession.builder.appName("kafka-to-iceberg")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,"
            "org.apache.spark:spark-protobuf_2.12:3.5.2,"
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
            "org.apache.iceberg:iceberg-aws-bundle:1.5.2,"
            "org.apache.hadoop:hadoop-aws:3.3.4",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", iceberg_catalog_uri)
        .config("spark.sql.catalog.iceberg.warehouse", iceberg_warehouse)
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg.s3.region", aws_region)
        .config("spark.sql.catalog.iceberg.s3.access-key-id", s3_access_key)
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", s3_secret_key)
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
        .option(
            "checkpointLocation",
            os.environ.get("CHECKPOINT_LOCATION", "/tmp/spark-consumer-checkpoint"),
        )
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
