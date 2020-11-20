package ru.rt.restream.reindexer.connector.binding.cproto;

import ru.rt.restream.reindexer.connector.exceptions.UnimplementedException;

import java.nio.charset.StandardCharsets;

/**
 * A byte buffer. This class defines methods for reading and writing values.
 */
public class ByteBuffer {

    private static final float DEFAULT_EXPAND_FACTOR = 1.5f;

    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    private byte[] buffer;

    private final float expandFactor;

    private int position = 0;

    private int size;

    /**
     * Wrap byte array in a buffer.
     */
    public ByteBuffer(byte[] bytes) {
        this(bytes, DEFAULT_EXPAND_FACTOR);
    }

    public ByteBuffer(byte[] buffer, float expandFactor) {
        this.buffer = buffer;
        this.expandFactor = expandFactor;
        position = buffer.length;
        size = buffer.length;
    }

    public ByteBuffer() {
        this(DEFAULT_EXPAND_FACTOR, DEFAULT_INITIAL_CAPACITY);
    }

    public ByteBuffer(float expandFactor, int initialCapacity) {
        this.expandFactor = expandFactor;
        buffer = new byte[initialCapacity];
    }

    public ByteBuffer writeUnsignedShort(int value) {
        writeIntBits(value, Short.BYTES);
        return this;
    }

    public ByteBuffer writeUnsignedInt(long value) {
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
        this.size = size + buffer.length;
    }

    public ByteBuffer writeUnsignedVarInt(long value) {
        grow(10);
        byte[] buffer = new byte[10];
        int valueBytes = writeValueToBuffer(buffer, value);
        System.arraycopy(buffer, 0, this.buffer, position, valueBytes);
        position = position + valueBytes;
        size = size + valueBytes;
        return this;
    }

    private int writeValueToBuffer(byte[] buffer, long value) {
        int i = 0;
        while (value >= 0x80) {
            buffer[i] = (byte) (value | 0x80);
            value >>= 7;
            i++;
        }
        buffer[i] = (byte) value;
        return i + 1;
    }

    public ByteBuffer writeVarInt(int value) {
        return writeUnsignedVarInt((value << 1) ^ (value >> 31));
    }

    public ByteBuffer writeVarLong(long value) {
        return writeUnsignedVarInt((value << 1) ^ (value >> 63));
    }

    public ByteBuffer writeString(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        int length = bytes.length;
        writeUnsignedVarInt(length);
        grow(length);
        System.arraycopy(bytes, 0, this.buffer, position, length);
        position = position + bytes.length;
        size = size + bytes.length;
        return this;
    }

    public ByteBuffer write(byte[] value) {
        int length = value.length;
        writeUnsignedVarInt(length);
        writeBytes(value);
        return this;
    }

    public ByteBuffer writeBytes(byte[] value) {
        grow(value.length);
        System.arraycopy(value, 0, this.buffer, position, value.length);
        position = position + value.length;
        size = size + value.length;
        return this;
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

    public int readUnsignedShort() {
        return (int) readIntBits(Short.BYTES);
    }

    public long readUnsignedInt() {
        return readIntBits(Integer.BYTES);
    }

    private long readIntBits(int size) {
        if (position + size > buffer.length) {
            final String msg = String.format("Buffer underflow error: position %d, length %d, need %d", position,
                    buffer.length, size);
            throw new RuntimeException(msg);
        }

        long value = 0;
        for (int i = size - 1; i >= 0; i--) {
            value = (buffer[position + i] & 0xFF) | (value << 8);
        }

        position += size;
        return value;
    }

    public int readUnsignedVarInt() {
        int value = 0;
        int i = 0;
        int b;
        while (((b = buffer[position++]) & 0x80) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 35) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
        }
        return value | (b << i);
    }

    public int readVarInt() {
        int raw = readUnsignedVarInt();
        // This undoes the trick in writeSignedVarInt()
        int temp = (((raw << 31) >> 31) ^ raw) >> 1;
        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values.
        // Must re-flip the top bit if the original read value had it set.
        return temp ^ (raw & (1 << 31));
    }

    public long readUnsignedVarLong() {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = buffer[position++]) & 0x80L) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 63) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
        }
        return value | (b << i);
    }

    public long readVarLong() {
        long raw = readUnsignedVarLong();
        // This undoes the trick in writeSignedVarLong()
        long temp = (((raw << 63) >> 63) ^ raw) >> 1;
        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values
        // Must re-flip the top bit if the original read value had it set.
        return temp ^ (raw & (1L << 63));
    }

    public String readVString() {
        int length = readUnsignedVarInt();
        return readString(length);
    }

    public String readString(int length) {
        byte[] bytes = new byte[length];
        System.arraycopy(buffer, position, bytes, 0, length);
        position += length;
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public double readDouble() {
        throw new UnimplementedException();
    }

    public byte[] readVBytes() {
        int length = readUnsignedVarInt();
        return readBytes(length);
    }

    public byte[] readBytes(int length) {
        byte[] bytes = new byte[length];
        System.arraycopy(buffer, position, bytes, 0, length);
        position += length;
        return bytes;
    }

    public byte[] readBytes() {
        byte[] bytes = new byte[buffer.length - position];
        System.arraycopy(buffer, position, bytes, 0, bytes.length);
        return bytes;
    }

    public byte[] bytes() {
        byte[] bytes = new byte[size];
        System.arraycopy(buffer, 0, bytes, 0, size);
        return bytes;
    }

    public void rewind() {
        position = 0;
    }

}
