package com.acme.spark;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;

import org.junit.jupiter.api.Test;

class ConfluentProtobufTest {

    @Test
    void stripConfluentProtobufFraming_returnsPayload_forValidFrameWithZeroIndexes() {
        byte[] payload = new byte[] {0x01, 0x02, 0x03};
        byte[] framed = frame(123, 0, payload);

        assertArrayEquals(payload, ConfluentProtobuf.stripConfluentProtobufFraming(framed));
    }

    @Test
    void stripConfluentProtobufFraming_returnsPayload_forValidFrameWithMultipleIndexes() {
        byte[] payload = new byte[] {(byte) 0xAA, (byte) 0xBB};
        byte[] framed = frame(7, 2, payload, 5, 300);

        assertArrayEquals(payload, ConfluentProtobuf.stripConfluentProtobufFraming(framed));
    }

    @Test
    void stripConfluentProtobufFraming_returnsNull_onNullInput() {
        assertNull(ConfluentProtobuf.stripConfluentProtobufFraming(null));
    }

    @Test
    void stripConfluentProtobufFraming_returnsNull_onTooShort() {
        assertNull(ConfluentProtobuf.stripConfluentProtobufFraming(new byte[] {0, 0, 0, 0, 0}));
    }

    @Test
    void stripConfluentProtobufFraming_returnsNull_onWrongMagicByte() {
        byte[] payload = new byte[] {0x01};
        byte[] framed = frame(1, 0, payload);
        framed[0] = 1;
        assertNull(ConfluentProtobuf.stripConfluentProtobufFraming(framed));
    }

    @Test
    void stripConfluentProtobufFraming_returnsNull_onMalformedVarintEof() {
        // Magic + schemaId + start of varint (continuation set) but no more bytes
        byte[] buf = new byte[] {0, 0, 0, 0, 1, (byte) 0x80};
        assertNull(ConfluentProtobuf.stripConfluentProtobufFraming(buf));
    }

    @Test
    void stripConfluentProtobufFraming_returnsNull_onNegativeLengthAfterZigZagDecode() {
        // Provide uvarint=1 -> zigzag decode => -1
        byte[] payload = new byte[] {0x01};
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0);
        writeSchemaId(out, 1);
        writeUVarint(out, 1);
        out.writeBytes(payload);

        assertNull(ConfluentProtobuf.stripConfluentProtobufFraming(out.toByteArray()));
    }

    private static byte[] frame(int schemaId, int indexCount, byte[] payload, int... indexes) {
        if (indexes.length != indexCount) {
            throw new IllegalArgumentException("indexCount must equal indexes.length");
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0);
        writeSchemaId(out, schemaId);

        int encodedLength = zigZagEncode(indexCount);
        writeUVarint(out, encodedLength);

        for (int idx : indexes) {
            writeUVarint(out, idx);
        }

        out.writeBytes(payload);
        return out.toByteArray();
    }

    private static void writeSchemaId(ByteArrayOutputStream out, int schemaId) {
        out.write((schemaId >>> 24) & 0xFF);
        out.write((schemaId >>> 16) & 0xFF);
        out.write((schemaId >>> 8) & 0xFF);
        out.write(schemaId & 0xFF);
    }

    private static int zigZagEncode(int n) {
        return (n << 1) ^ (n >> 31);
    }

    private static void writeUVarint(ByteArrayOutputStream out, int value) {
        long v = value & 0xFFFFFFFFL;
        while ((v & ~0x7FL) != 0) {
            out.write((int) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        out.write((int) v);
    }
}
