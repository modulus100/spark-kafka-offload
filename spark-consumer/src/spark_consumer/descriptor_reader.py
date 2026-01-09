from __future__ import annotations

import os
import uuid

from pyspark.sql import SparkSession


def local_descriptor_path(spark: SparkSession, path: str) -> str:
    p = (path or "").strip()
    if not p:
        return p

    if p.startswith("s3://"):
        p = "s3a://" + p[len("s3://") :]

    if p.startswith("s3a://"):
        jvm = spark.sparkContext._jvm
        hconf = spark.sparkContext._jsc.hadoopConfiguration()
        src = jvm.org.apache.hadoop.fs.Path(p)
        dst = jvm.org.apache.hadoop.fs.Path(f"file:/tmp/proto-desc-{uuid.uuid4()}.desc")
        fs = src.getFileSystem(hconf)
        fs.copyToLocalFile(False, src, dst)
        return dst.toString().replace("file:", "")

    return os.path.abspath(p)
