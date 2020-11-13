package ru.rt.restream.reindexer.connector.binding.cproto;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Serializer {

    private static final float DEFAULT_EXPAND_FACTOR = 1.5f;

    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    private final float expandFactor;

    private byte[] buffer;

    private int position = 0;

    public Serializer() {
        this(DEFAULT_EXPAND_FACTOR, DEFAULT_INITIAL_CAPACITY);
    }

    public Serializer(float expandFactor, int initialCapacity) {
        this.expandFactor = expandFactor;
        buffer = new byte[initialCapacity];
    }

    public Serializer writeUnsignedShort(int value) {
        writeIntBits(value, Short.BYTES);
        return this;
    }

    public Serializer writeUnsignedInt(long value) {
        writeIntBits(value, Integer.BYTES);
        return this;
    }

    private void writeIntBits(long input, int size) {
        grow(size);
        byte[] buffer = new byte[size];
        for (int i = 0; i < size; i++) {
            buffer[i] = (byte) input;
            input = input >> 8;
        }
        System.arraycopy(buffer, 0, this.buffer, position, buffer.length);
        position = position + buffer.length;
    }

    public Serializer writeUnsignedVarInt(long value) {
        grow(10);
        byte[] buffer = new byte[10];
        int valueBytes = writeValueToBuffer(buffer, value);
        System.arraycopy(buffer, 0, this.buffer, position, valueBytes);
        position = position + valueBytes;
        return this;
    }

    private int writeValueToBuffer(byte[] buffer, long value) {
        int i = 0;
        while (value >= 0x80) {
            buffer[i] = (byte)(value | 0x80);
            value >>= 7;
            i++;
        }
        buffer[i] = (byte)value;
        return i + 1;
    }

    public Serializer writeVarInt(int value) {
        return writeUnsignedVarInt((value << 1) ^ (value >> 31));
    }

    public Serializer writeString(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        int length = bytes.length;
        writeUnsignedVarInt(length);
        grow(length);
        System.arraycopy(bytes, 0, this.buffer, position, length);
        position = position + bytes.length;
        return this;
    }

    public Serializer write(byte[] value) {
        int length = value.length;
        writeUnsignedVarInt(length);
        writeBytes(value);
        return this;
    }

    public Serializer writeBytes(byte[] value) {
        grow(value.length);
        System.arraycopy(value, 0, this.buffer, position, value.length);
        position = position + value.length;
        return this;
    }

    public byte[] bytes() {
        return Arrays.copyOf(buffer, position);
    }

    private void grow(int need) {
        if (buffer.length - position < need) {
            int newCapacity = (int) (buffer.length * expandFactor);
            while (newCapacity < (buffer.length + need)) {
                newCapacity *= expandFactor;
            }
            byte[] expanded = new byte[newCapacity];
            System.arraycopy(buffer, 0, expanded, 0, position);
            buffer = expanded;
        }
    }

}
