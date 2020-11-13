package ru.rt.restream.reindexer.connector.binding.cproto;

import lombok.extern.slf4j.Slf4j;
import ru.rt.restream.reindexer.connector.binding.Consts;
import ru.rt.restream.reindexer.connector.exceptions.InvalidProtocolException;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static ru.rt.restream.reindexer.connector.binding.cproto.Cproto.*;

@Slf4j
public class RpcDecoder {

    public RpcResult decode(DataInputStream inputStream) throws IOException {
        byte[] header = new byte[CPROTO_HDR_LEN];
        inputStream.readFully(header);
        Deserializer headerDeserializer = new Deserializer(header);
        long magic = headerDeserializer.readUnsignedInt();
        if (magic != CPROTO_MAGIC) {
            throw new InvalidProtocolException(String.format("Invalid cproto magic '%08X'", magic));
        }
        int version = headerDeserializer.readUnsignedShort();
        headerDeserializer.readUnsignedShort();
        int size = (int) headerDeserializer.readUnsignedInt();
        long rseq = headerDeserializer.readUnsignedShort();

        boolean compressed = (version & CPROTO_VERSION_COMPRESSION_FLAG) != 0;
        version &= CPROTO_VERSION_MASK;
        if (version < CPROTO_VERSION) {
            throw new InvalidProtocolException(String.format("Unsupported cproto version '%04X'. " +
                    "This client expects reindexer server v1.9.8+", version));
        }

        byte[] body = new byte[size];
        inputStream.readFully(body);
        Deserializer deserializer = new Deserializer(body);
        int code = deserializer.readUnsignedVarInt();
        String message = deserializer.readString();

        RpcResult.Error error;
        if (code != Consts.ERR_OK) {
            error = new RpcResult.Error(code, message);
        } else {
            error = new RpcResult.Error(code, "OK");
        }

        List<Object> args = new ArrayList<>();
        int argsCount = deserializer.readUnsignedVarInt();
        for (int i = 0; i < argsCount; i++) {
            args.add(readArgument(deserializer));
        }
        return new RpcResult(error, args);
    }

    private Object readArgument(Deserializer deserializer) throws IOException {
        int type = deserializer.readUnsignedVarInt();
        switch (type) {
            case Consts.VALUE_INT:
                return deserializer.readVarInt();
            case Consts.VALUE_BOOL:
                return deserializer.readVarInt() != 0;
            case Consts.VALUE_STRING:
                return deserializer.readString();
            case Consts.VALUE_INT_64:
                return deserializer.readVarLong();
            case Consts.VALUE_DOUBLE:
                return deserializer.readDouble();
            default:
                throw new InvalidProtocolException(String.format("cproto: Unexpected arg type %d", type));
        }
    }

}
