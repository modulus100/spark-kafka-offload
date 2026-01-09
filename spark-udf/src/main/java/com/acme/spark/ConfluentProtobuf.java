package com.acme.spark;

public final class ConfluentProtobuf {

    private ConfluentProtobuf() {}

    public static byte[] stripConfluentProtobufFraming(byte[] value) {
        if (value == null || value.length < 6) {
            return null;
        }

        // Confluent framing:
        // 0: magic byte (0)
        // 1..4: schema id (big endian)
        // 5..: index array (varint encoded; zig-zag)
        // remaining: protobuf payload
        if (value[0] != 0) {
            return null;
        }

        int idx = 5;
        try {
            Varint lengthVarint = readUVarint(value, idx);
            int lengthZigZag = lengthVarint.value;
            idx = lengthVarint.nextIndex;
            int length = zigZagDecode(lengthZigZag);
            if (length < 0) {
                return null;
            }
            for (int i = 0; i < length; i++) {
                Varint ignored = readUVarint(value, idx);
                idx = ignored.nextIndex;
            }
        } catch (RuntimeException e) {
            return null;
        }

        if (idx > value.length) {
            return null;
        }

        byte[] payload = new byte[value.length - idx];
        System.arraycopy(value, idx, payload, 0, payload.length);
        return payload;
    }

    private static final class Varint {
        final int value;
        final int nextIndex;

        private Varint(int value, int nextIndex) {
            this.value = value;
            this.nextIndex = nextIndex;
        }
    }

    private static Varint readUVarint(byte[] buf, int start) {
        long value = 0;
        int shift = 0;
        int i = start;
        while (true) {
            if (i >= buf.length) {
                throw new RuntimeException("EOF");
            }
            int b = buf[i++] & 0xFF;
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
            if (shift > 35) {
                throw new RuntimeException("varint too long");
            }
        }
        if (value > Integer.MAX_VALUE) {
            throw new RuntimeException("varint overflow");
        }
        return new Varint((int) value, i);
    }

    private static int zigZagDecode(int n) {
        return (n >>> 1) ^ -(n & 1);
    }
}
