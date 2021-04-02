/*
 * Copyright 2020 Restream
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.rt.restream.reindexer.binding.cproto;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A byte buffer with auto-expansion functionality. This class defines methods for reading and writing values with space
 * checks to put-methods. When a put-method is invoked, a check is made to determine if there is enough room within the
 * buffer to accommodate the data to be added. If so, data is put into the buffer as usual. If not, the buffer is
 * expanded by a defined expansion factor until the new capacity is big enough to contain both the contents of the old
 * (current) buffer and the data to be added.
 * <p>
 * This class is not thread safe. If multiple threads invoke a put-operation on this buffer at the same time, the buffer
 * could expand multiple times if not synchronized properly.
 */
public class ByteBuffer {

    private static final float DEFAULT_EXPAND_FACTOR = 1.5f;

    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    private byte[] buffer;

    private final float expandFactor;

    private int position;

    private int size;

    /**
     * Wraps byte array in a buffer with default expand factor.
     * Increments buffer position.
     *
     * @param bytes array that will be backed
     */
    public ByteBuffer(byte[] bytes) {
        this(bytes, DEFAULT_EXPAND_FACTOR);
    }

    /**
     * Wraps byte array in a buffer with specified expand factor.
     * Increments buffer position.
     *
     * @param bytes        array that will be backed
     * @param expandFactor value, that will be used when buffer will be expanding
     */
    public ByteBuffer(byte[] bytes, float expandFactor) {
        this.buffer = bytes;
        this.expandFactor = expandFactor;
        position = bytes.length;
        size = bytes.length;
    }

    /**
     * Construct byte buffer with default expand factor and initial capacity.
     * Position is set to 0.
     */
    public ByteBuffer() {
        this(DEFAULT_EXPAND_FACTOR, DEFAULT_INITIAL_CAPACITY);
    }

    /**
     * Construct byte buffer with default expand factor and specified initial capacity.
     * Position is set to 0.
     *
     * @param initialCapacity initial capacity of backed byte array
     */
    public ByteBuffer(int initialCapacity) {
        this(DEFAULT_EXPAND_FACTOR, initialCapacity);
    }

    /**
     * Constructs byte buffer with specified expand factor and initial capacity.
     * Position is set to 0.
     *
     * @param expandFactor    value, which is used to allocate new backed array.
     * @param initialCapacity initial capacity of backed byte array
     */
    public ByteBuffer(float expandFactor, int initialCapacity) {
        this.expandFactor = expandFactor;
        buffer = new byte[initialCapacity];
    }

    /**
     * Encodes an integer value into unsigned 16-bit integer.
     * Increments buffer position.
     *
     * @param value value to encode
     * @return the {@link ByteBuffer} for further customizations
     */
    public ByteBuffer putUInt16(int value) {
        if (value < 0 || value > 0xFFFF) {
            throw new IllegalArgumentException();
        }
        putIntBits(value, Short.BYTES, -1);
        return this;
    }

    /**
     * Encodes an integer value into unsigned 32-bit integer.
     * Increments buffer position.
     *
     * @param value value to encode
     * @return the {@link ByteBuffer} for further customizations
     */
    public ByteBuffer putUInt32(long value) {
        if (value < 0 || value > 0xFFFF_FFFFL) {
            throw new IllegalArgumentException();
        }
        putIntBits(value, Integer.BYTES, -1);
        return this;
    }

    public ByteBuffer putUInt32(long value, int position) {
        if (value < 0 || value > 0xFFFF_FFFFL) {
            throw new IllegalArgumentException();
        }
        putIntBits(value, Integer.BYTES, position);
        return this;
    }

    private void putIntBits(long input, int size, int position) {
        grow(size);
        byte[] buffer = new byte[size];
        for (int i = 0; i < size; i++) {
            buffer[i] = (byte) input;
            input = input >> 8;
        }
        if (position != -1) {
            System.arraycopy(buffer, 0, this.buffer, position, buffer.length);
        } else {
            System.arraycopy(buffer, 0, this.buffer, this.position, buffer.length);
            this.position = this.position + buffer.length;
            this.size = this.size + buffer.length;
        }
    }

    /**
     * Encodes a value using the variable-length encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a>. Zig-zag is not used, so input must not be negative.
     * If values can be negative, use {@link #putVarInt64(long)} instead.
     * Increments buffer position.
     *
     * @param value value to encode
     * @return the {@link ByteBuffer} for further customizations
     */
    public ByteBuffer putVarUInt32(long value) {
        if (value < 0) {
            throw new IllegalArgumentException();
        }
        grow(10);
        byte[] buffer = new byte[10];
        int variantSize = putUVariant(buffer, value);
        System.arraycopy(buffer, 0, this.buffer, position, variantSize);
        position = position + variantSize;
        size = size + variantSize;
        return this;
    }

    /**
     * Encodes a value using the variable-length encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a>. It uses zig-zag encoding to efficiently
     * encode signed values. If values are known to be nonnegative,
     * {@link #putUInt32(long)} should be used.
     * Increments buffer position.
     *
     * @param value value to encode
     * @return the {@link ByteBuffer} for further customizations
     */
    public ByteBuffer putVarInt64(long value) {
        // Great trick from http://code.google.com/apis/protocolbuffers/docs/encoding.html#types
        return putVarUInt32((value << 1) ^ (value >> 63));
    }

    private int putUVariant(byte[] buffer, long value) {
        int i = 0;
        while (value >= 0x80) {
            buffer[i] = (byte) (value | 0x80);
            value >>= 7;
            i++;
        }
        buffer[i] = (byte) value;
        return i + 1;
    }

    /**
     * Encodes a string value. Inserts encoded length of specified string and
     * then inserts string bytes in the backed byte array.
     * Increments buffer position.
     *
     * @param value value to encode
     * @return the {@link ByteBuffer} for further customizations
     */
    public ByteBuffer putVString(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        putVBytes(bytes);
        return this;
    }

    /**
     * Puts a byte array into buffer. Inserts encoded length of specified byte
     * array and then inserts string bytes in the backed byte array.
     * Increments buffer position.
     *
     * @param value array to put
     * @return the {@link ByteBuffer} for further customizations
     */
    public ByteBuffer putVBytes(byte[] value) {
        int length = value.length;
        putVarUInt32(length);
        writeBytes(value);
        return this;
    }

    /**
     * Writes specified byte array into buffer.
     * Increments buffer position.
     *
     * @param value array to put
     * @return the {@link ByteBuffer} for further customizations
     */
    public ByteBuffer writeBytes(byte[] value) {
        grow(value.length);
        System.arraycopy(value, 0, this.buffer, position, value.length);
        position = position + value.length;
        size = size + value.length;
        return this;
    }

    /**
     * Read an unsigned 16-bit integer from the current position in the buffer.
     * Increments buffer position.
     *
     * @return the integer read, as an int to avoid signedness
     */
    public int getUInt16() {
        return (int) readIntBits(Short.BYTES);
    }

    /**
     * Reads an unsigned 32-bit integer from the current position in the buffer.
     * Increments buffer position.
     *
     * @return the integer read, as a long to avoid signedness
     */
    public long getUInt32() {
        return readIntBits(Integer.BYTES);
    }

    /**
     * Reads an unsigned 64-bit integer from the current position in the buffer.
     * Increments buffer position.
     *
     * @return the long read
     */
    public long getUInt64() {
        return readIntBits(Long.BYTES);
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

    /**
     * Reads an unsigned variable length integer from a buffer.
     * Increments buffer position.
     *
     * @return the integer read, as a long to avoid signedness
     */
    public long getVarUInt() {
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

    /**
     * Reads a signed variable length integer from a buffer.
     * Increments buffer position.
     *
     * @return signed long
     */
    public long getVarInt() {
        long raw = getVarUInt();
        // This undoes the trick in writeSignedVarLong()
        long temp = (((raw << 63) >> 63) ^ raw) >> 1;
        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values
        // Must re-flip the top bit if the original read value had it set.
        return (temp ^ (raw & (1L << 63)));
    }

    /**
     * Reads a variable length string from a buffer..
     * Increments buffer position.
     *
     * @return the string read from a backed array
     */
    public String getVString() {
        int length = (int) getVarUInt();
        return getString(length);
    }

    /**
     * Reads a string of specified length from a buffer.
     * Increments buffer position.
     *
     * @param length the length of a string
     * @return the string read from a backed array
     */
    private String getString(int length) {
        byte[] bytes = new byte[length];
        System.arraycopy(buffer, position, bytes, 0, length);
        position += length;
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Reads double value from a buffer. Increments buffer position.
     *
     * @return the double read from a backed array
     */
    public double getDouble() {
        return Double.longBitsToDouble(readIntBits(Long.BYTES));
    }

    /**
     * Reads byte array from a buffer. The length of array is encoded into backed array.
     * Increments buffer position.
     *
     * @return bytes read from a backed array
     */
    public byte[] getVBytes() {
        int length = (int) getVarUInt();
        return getBytes(length);
    }

    /**
     * Reads byte array of specified size from a buffer.
     * Increments buffer position.
     *
     * @param length the length of an array of bytes
     * @return bytes read from a backed array
     */
    public byte[] getBytes(int length) {
        byte[] bytes = new byte[length];
        System.arraycopy(buffer, position, bytes, 0, length);
        position += length;
        return bytes;
    }

    /**
     * Reads all remaining bytes from a buffer.
     * Increments buffer position.
     *
     * @return bytes read from a backed array
     */
    public byte[] getBytes() {
        byte[] bytes = new byte[buffer.length - position];
        System.arraycopy(buffer, position, bytes, 0, bytes.length);
        return bytes;
    }

    /**
     * Returns all used bytes from the backed array.
     * Doesn't increments buffer position.
     *
     * @return bytes read from a backed array
     */
    public byte[] bytes() {
        byte[] bytes = new byte[size];
        System.arraycopy(buffer, 0, bytes, 0, size);
        return bytes;
    }

    /**
     * Sets current buffer position at the beginning of the backed array.
     *
     * @return the {@link ByteBuffer} for further customizations
     */
    public ByteBuffer rewind() {
        position = 0;
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

    public void putDouble(Double value) {
        putIntBits(Double.doubleToLongBits(value), Long.BYTES, -1);
    }

    public void truncateStart(int length) {
        byte[] bytes = new byte[buffer.length - length];
        System.arraycopy(buffer, length, bytes, 0, buffer.length - length);
        buffer = bytes;
        position = position - length;
        size = size - length;
    }

    /**
     * Resets the buffer to be empty,
     * but it retains the underlying storage for use by future writes.
     */
    public void reset() {
        Arrays.fill(buffer, (byte) 0);
        position = 0;
        size = 0;
    }

    /**
     * Skip bytes and increase position by 'length'.
     *
     * @param length bytes to skip
     */
    public void skip(int length) {
        position += length;
    }

    /**
     * Return current cursor position in byte buffer.
     *
     * @return current cursor position
     */
    public int getPosition() {
        return position;
    }

    /**
     * Return the size of recorded bytes.
     *
     * @return size of recorded bytes
     */
    public int length() {
        return size;
    }

}
