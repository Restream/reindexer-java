package ru.rt.restream.reindexer.connector.binding.cproto;

import lombok.SneakyThrows;
import ru.rt.restream.reindexer.connector.binding.Consts;
import ru.rt.restream.reindexer.connector.exceptions.InvalidProtocolException;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RPCEncoder {
    private static final int CPROTO_MAGIC = 0xEEDD1132;
    private static final int CPROTO_VERSION = 0x101;
    private static final int CPROTO_HDR_LEN = 16;
    private static final int CPROTO_VERSION_COMPRESSION_FLAG = 1 << 10;
    private static final int CPROTO_VERSION_MASK = 0x3FF;

    public byte[] encode(int command, int seq, Object... args) {
        Serializer body = ByteArraySerializer.newSerializer();
        body.putVarUInt(args.length);
        for (Object arg : args) {
            if (arg instanceof Boolean) {
                body.putVarUInt(Consts.VALUE_BOOL)
                        .putVarUInt((Boolean) arg ? 1L : 0L);
            } else if (arg instanceof Short) {
                body.putVarUInt(Consts.VALUE_INT)
                        .putVarInt(((Short) arg).longValue());
            } else if (arg instanceof Integer) {
                body.putVarUInt(Consts.VALUE_INT)
                        .putVarInt(((Integer) arg).longValue());
            } else if (arg instanceof Long) {
                throw new UnsupportedOperationException();
            } else if (arg instanceof String) {
                body.putVarUInt(Consts.VALUE_STRING)
                        .putVString((String) arg);
            } else if (arg instanceof ByteBuffer) {
                body.putVarUInt(Consts.VALUE_STRING)
                        .putVBytes((ByteBuffer) arg);
            }
        }

        Serializer header = getHeader(seq, command, body);
        byte[] bytes = new byte[header.bytes().length + body.bytes().length];

        System.arraycopy(header.bytes(), 0, bytes, 0, header.bytes().length);
        System.arraycopy(body.bytes(), 0, bytes, header.bytes().length, body.bytes().length);
        return bytes;
    }

    private Serializer getHeader(int seq, int command, Serializer body) {
        return ByteArraySerializer.newSerializer()
                .putUInt32(CPROTO_MAGIC)
                .putUInt16(CPROTO_VERSION)
                .putUInt16(command)
                .putUInt32(body.bytes().length)
                .putUInt32(seq);
    }

    public RPCResult decode(DataInputStream inputStream) throws IOException {
        byte[] headerBytes = new byte[CPROTO_HDR_LEN];
        inputStream.readFully(headerBytes);
        ByteArraySerializer headerSerializer = ByteArraySerializer.getSerializer(headerBytes);

        int magic = (int) headerSerializer.getUInt32();
        if (magic != CPROTO_MAGIC) {
            throw new InvalidProtocolException(String.format("Invalid cproto magic '%08X'", magic));
        }

        int version = headerSerializer.getUInt16();
        headerSerializer.getUInt16();
        int size = (int) headerSerializer.getUInt32();
        long rseq = headerSerializer.getUInt32();

        boolean compressed = (version & CPROTO_VERSION_COMPRESSION_FLAG) != 0;
        version &= CPROTO_VERSION_MASK;
        if (version < CPROTO_VERSION) {
            throw new InvalidProtocolException(String.format("Unsupported cproto version '%04X'. " +
                    "This client expects reindexer server v1.9.8+", version));
        }

        byte[] answer = new byte[size];
        inputStream.readFully(answer);

        ByteArraySerializer answerSerializer = ByteArraySerializer.getSerializer(answer);
        int code = (int) answerSerializer.getVarUInt();
        String message = answerSerializer.getVString();

        RPCResult.Error error;
        if (code != Consts.ERR_OK) {
            error = new RPCResult.Error(code, message);
        } else {
            error = new RPCResult.Error(code, "OK");
        }

        List<Object> args = new ArrayList<>();
        int argsCount = (int) answerSerializer.getVarUInt();
        for (int i = 0; i < argsCount; i++) {
            args.add(readArgument(answerSerializer));
        }

        return new RPCResult(error, args);
    }

    private Object readArgument(ByteArraySerializer serializer) {
        int type = (int) serializer.getVarUInt();
        switch (type) {
            case Consts.VALUE_INT:
                return (int) serializer.getVarInt();
            case Consts.VALUE_BOOL:
                return serializer.getVarInt() != 0;
            case Consts.VALUE_STRING:
                return serializer.getVString();
            case Consts.VALUE_INT_64:
                return serializer.getVarInt();
            case Consts.VALUE_DOUBLE:
                return serializer.getDouble();
            default:
                throw new InvalidProtocolException(String.format("cproto: Unexpected arg type %d", type));
        }
    }

}
