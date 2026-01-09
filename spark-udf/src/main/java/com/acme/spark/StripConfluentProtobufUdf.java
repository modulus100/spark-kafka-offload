package com.acme.spark;

import org.apache.spark.sql.api.java.UDF1;

public class StripConfluentProtobufUdf implements UDF1<byte[], byte[]> {

    @Override
    public byte[] call(byte[] value) {
        return ConfluentProtobuf.stripConfluentProtobufFraming(value);
    }
}
