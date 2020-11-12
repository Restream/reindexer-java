package ru.rt.restream.reindexer.connector.binding.cproto;


import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ByteArraySerializer implements Serializer {

    byte[] buffer = new byte[0];
    int position;

    private ByteArraySerializer() {
    }

    private ByteArraySerializer(byte[] buffer) {
        this.buffer = Arrays.copyOf(buffer, buffer.length);
    }

    public static ByteArraySerializer getSerializer(byte[] buffer) {
        return new ByteArraySerializer(buffer);
    }

    public static ByteArraySerializer newSerializer() {
        return new ByteArraySerializer();
    }

    @Override
    public byte[] bytes() {
        return buffer;
    }

    /**
     * Читает без знака 2 байта (int16).
     * <p>
     * В Java для беззнаковых надо использовать более широкий тип.
     */
    @Override
    public int getUInt16() {
        return (int) readIntBits(Short.BYTES);
    }

    /**
     * Читает без знака 4 байта (int32).
     * <p>
     * В Java для беззнаковых нужно использовать более широкий тип.
     */
    @Override
    public long getUInt32() {
        return readIntBits(Integer.BYTES);
    }

    public long readIntBits(int size) {
        if (position + size > buffer.length) {
            String format = String.format("Internal error: serializer need %d bytes, but only %d available",
                    position + size, buffer.length - position);
            throw new RuntimeException(format);
        }
        long value = 0;
        for (int i = size - 1; i >= 0; i--) {
            value = (buffer[i + position] & 0xFF) | (value << 8);
        }
        position += size;
        return value;
    }

    @Override
    public Serializer putUInt16(int input) {
        writeIntBits(input, Short.BYTES);
        return this;
    }

    @Override
    public Serializer putUInt32(long input) {
        writeIntBits(input, Integer.BYTES);
        return this;
    }

    @Override
    public void writeIntBits(long input, int size) {
        int length = buffer.length;
        buffer = Arrays.copyOf(buffer, length + size);
        for (int i = 0; i < size; i++) {
            buffer[length + i] = (byte) input;
            input = input >> 8;
        }
    }

    @Override
    public Serializer putVarUInt(long input) {
        int length = buffer.length;
        byte[] bytes = new byte[10];
        int rl = putUvarint(bytes, input);
        buffer = Arrays.copyOf(buffer, length + rl);
        if (rl >= 0) {
            System.arraycopy(bytes, 0, buffer, length, rl);
        }
        return this;
    }

    // https://en.wikipedia.org/wiki/Variable-length_quantity
    private int putUvarint(byte[] buffer, long value) {
        int i = 0;
        while (value >= 0x80) {
            buffer[i] = (byte) (value | 0x80);
            value >>= 7;
            i++;
        }
        buffer[i] = (byte) value;
        return i + 1;
    }

    @Override
    public Serializer putVarInt(long input) {
        // Great trick from http://code.google.com/apis/protocolbuffers/docs/encoding.html#types
        putVarUInt((input << 1) ^ (input >> 31));
        return this;
    }

    @Override
    public Serializer putVString(String input) {
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
        int length = bytes.length;
        putVarUInt(length);

        int l = buffer.length;
        buffer = Arrays.copyOf(buffer, l + length);

        System.arraycopy(bytes, 0, buffer, l, length);
        return this;
    }

    @Override
    public Serializer putVBytes(ByteBuffer input) {
        byte[] array = input.array();
        int length = array.length;
        putVarUInt(length);
        int l = this.buffer.length;
        this.buffer = Arrays.copyOf(this.buffer, l + length);
        System.arraycopy(array, 0, this.buffer, l, length);
        return this;
    }

    @Override
    public Serializer write(ByteBuffer input) {
        byte[] ar = input.array();
        int sl = ar.length;
        int l = buffer.length;
        buffer = Arrays.copyOf(buffer, l + sl);
        System.arraycopy(ar, 0, buffer, l, sl);
        return this;
    }

    @Override
    public long getVarInt() {
        long raw = getVarUInt();
        // This undoes the trick in writeSignedVarLong()
        long temp = (((raw << 63) >> 63) ^ raw) >> 1;
        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values
        // Must re-flip the top bit if the original read value had it set.
        return temp ^ (raw & (1L << 63));
    }

    @Override
    public long getVarUInt() {
        long x = 0;
        int s = 0;
        int c = 0;

        for (int i = position; i < buffer.length; i++) {
            c++;
            final long b = buffer[i] & 0xFF;
            if (s >= 63) {
                if (((s == 63) && (b > 1)) || (s > 63))
                    throw new NumberFormatException("Overflow: value is larger than 64 bits");
            }
            if ((b & 0x80) == 0) {
                position = position + c;
                return x | (b << s);
            }
            x |= ((b & 0x7F) << s);
            s += 7;
        }
        throw new IllegalArgumentException("Input buffer too small");
    }

    @Override
    public String getVString() {
        int length = (int) getVarUInt();
        if (position + length > buffer.length) {
            throw new RuntimeException(String.format("Internal error: serializer need %d bytes, but only %d available",
                    length, buffer.length - position));
        }
        byte[] stringBytes = Arrays.copyOfRange(buffer, position, position + length);
        String v = new String(stringBytes);
        position += length;
        return v;
    }

    @Override
    public Double getDouble() {
        return null;
    }
}
