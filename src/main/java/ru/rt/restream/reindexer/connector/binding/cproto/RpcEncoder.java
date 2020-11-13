package ru.rt.restream.reindexer.connector.binding.cproto;

import ru.rt.restream.reindexer.connector.binding.Consts;

import java.nio.ByteBuffer;

import static ru.rt.restream.reindexer.connector.binding.cproto.Cproto.CPROTO_MAGIC;
import static ru.rt.restream.reindexer.connector.binding.cproto.Cproto.CPROTO_VERSION;

public class RpcEncoder {

    public byte[] encode(int command, int seq, Object... args) {

        byte[] body = getBodyBytes(args);
        byte[] header = getHeaderBytes(seq, command, body.length);
        byte[] bytes = new byte[header.length + body.length];
        System.arraycopy(header, 0, bytes, 0, header.length);
        System.arraycopy(body, 0, bytes, header.length, body.length);
        return bytes;
    }

    private byte[] getBodyBytes(Object[] args) {
        Serializer bodySerializer = new Serializer();
        bodySerializer.writeUnsignedVarInt(args.length);
        for (Object arg : args) {
            if (arg instanceof Boolean) {
                bodySerializer.writeUnsignedVarInt(Consts.VALUE_BOOL)
                        .writeUnsignedVarInt((Boolean) arg ? 1L : 0L);
            } else if (arg instanceof Short) {
                bodySerializer.writeUnsignedVarInt(Consts.VALUE_INT)
                        .writeVarInt(((Short) arg));
            } else if (arg instanceof Integer) {
                bodySerializer.writeUnsignedVarInt(Consts.VALUE_INT)
                        .writeVarInt(((Integer) arg));
            } else if (arg instanceof Long) {
                throw new UnsupportedOperationException();
            } else if (arg instanceof String) {
                bodySerializer.writeUnsignedVarInt(Consts.VALUE_STRING)
                        .writeString((String) arg);
            } else if (arg instanceof ByteBuffer) {
                bodySerializer.writeUnsignedVarInt(Consts.VALUE_STRING)
                        .write(((ByteBuffer) arg).array());
            }
        }

        return bodySerializer.bytes();
    }

    private byte[] getHeaderBytes(int seq, int command, int size) {
        return new Serializer()
                .writeUnsignedInt(CPROTO_MAGIC)
                .writeUnsignedShort(CPROTO_VERSION)
                .writeUnsignedShort(command)
                .writeUnsignedInt(size)
                .writeUnsignedInt(seq)
                .bytes();
    }

}
