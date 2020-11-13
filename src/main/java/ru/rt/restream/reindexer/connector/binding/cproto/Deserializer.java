package ru.rt.restream.reindexer.connector.binding.cproto;

import ru.rt.restream.reindexer.connector.exceptions.UnimplementedException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Deserializer {

    private final byte[] data;

    private int position = 0;

    public Deserializer(byte[] data) {
        this.data = data;
    }

    public int readUnsignedShort() {
        return (int) readIntBits(Short.BYTES);
    }

    public long readUnsignedInt() {
        return readIntBits(Integer.BYTES);
    }

    private long readIntBits(int size) {
        if (position + size > data.length) {
            final String msg = String.format("Buffer underflow error: position %d, length %d, need %d", position,
                    data.length, size);
            throw new RuntimeException(msg);
        }

        long value = 0;
        for (int i = size - 1; i >= 0; i--) {
            value = (data[position + i] & 0xFF) | (value << 8);
        }

        position += size;
        return value;
    }

    public int readUnsignedVarInt() {
        int value = 0;
        int i = 0;
        int b;
        while (((b = data[position++]) & 0x80) != 0) {
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
        while (((b = data[position++]) & 0x80L) != 0) {
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

    public String readString() {
        int stringLength = readUnsignedVarInt();
        byte[] stringBytes = new byte[stringLength];
        System.arraycopy(data, position, stringBytes, 0, stringLength);
        position += stringLength;
        return new String(stringBytes, StandardCharsets.UTF_8);
    }

    public double readDouble() {
        throw new UnimplementedException();
    }
}
