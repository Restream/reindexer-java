package ru.rt.restream.reindexer.connector.binding.cproto;

import java.nio.ByteBuffer;

public interface Serializer {
    byte[] bytes();

    int getUInt16();

    long getUInt32();

    Serializer putUInt16(int input);

    Serializer putUInt32(long input);

    void writeIntBits(long input, int sz);

    Serializer putVarUInt(long input);

    Serializer putVarInt(long input);

    Serializer putVString(String input);

    long getVarUInt();

    long getVarInt();

    String getVString();

    Double getDouble();

    Serializer putVBytes(ByteBuffer input);

    Serializer write(ByteBuffer input);
}
