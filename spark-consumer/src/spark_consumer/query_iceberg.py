from __future__ import annotations

import os

from pyspark.sql import SparkSession


def main() -> None:
    catalog = os.environ.get("ICEBERG_CATALOG", "iceberg")
    namespace = os.environ.get("ICEBERG_NAMESPACE", "demo")
    table = os.environ.get("ICEBERG_TABLE", "demo_events")
    limit = int(os.environ.get("ICEBERG_QUERY_LIMIT", "50"))

    iceberg_catalog_uri = os.environ.get("ICEBERG_CATALOG_URI", "http://localhost:8181")
    iceberg_warehouse = os.environ.get("ICEBERG_WAREHOUSE", "s3://datalake/warehouse")

    s3_endpoint = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
    s3_access_key = os.environ.get("S3_ACCESS_KEY", "admin")
    s3_secret_key = os.environ.get("S3_SECRET_KEY", "password")
    aws_region = os.environ.get("AWS_REGION", "us-east-1")

    spark = (
        SparkSession.builder.appName("query-iceberg")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
            "org.apache.iceberg:iceberg-aws-bundle:1.5.2,"
            "org.apache.hadoop:hadoop-aws:3.3.4",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "rest")
        .config(f"spark.sql.catalog.{catalog}.uri", iceberg_catalog_uri)
        .config(f"spark.sql.catalog.{catalog}.warehouse", iceberg_warehouse)
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog}.s3.endpoint", s3_endpoint)
        .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{catalog}.s3.region", aws_region)
        .config(f"spark.sql.catalog.{catalog}.s3.access-key-id", s3_access_key)
        .config(f"spark.sql.catalog.{catalog}.s3.secret-access-key", s3_secret_key)
        .getOrCreate()
    )

    spark.sql(f"SHOW NAMESPACES IN {catalog}").show(truncate=False)
    spark.sql(f"SHOW TABLES IN {catalog}.{namespace}").show(truncate=False)

    full_name = f"{catalog}.{namespace}.{table}"
    spark.sql(f"SELECT * FROM {full_name} ORDER BY ingested_at DESC LIMIT {limit}").show(
        truncate=False
    )

    spark.stop()


if __name__ == "__main__":
    main()
