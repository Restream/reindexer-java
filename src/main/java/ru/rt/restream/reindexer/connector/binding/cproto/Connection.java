package ru.rt.restream.reindexer.connector.binding.cproto;

import ru.rt.restream.reindexer.connector.binding.Consts;
import ru.rt.restream.reindexer.connector.exceptions.InvalidProtocolException;
import ru.rt.restream.reindexer.connector.exceptions.NetworkException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class Connection implements AutoCloseable {

    static final long CPROTO_MAGIC = 0xEEDD1132L;

    static final int CPROTO_VERSION = 0x101;

    static final int CPROTO_HDR_LEN = 16;

    static final int CPROTO_VERSION_COMPRESSION_FLAG = 1 << 10;

    static final int CPROTO_VERSION_MASK = 0x3FF;

    private final Socket clientSocket;
    private final DataOutputStream output;
    private final DataInputStream input;
    private int seq;

    public Connection(String host, int port) {
        try {
            clientSocket = new Socket(host, port);
            output = new DataOutputStream(clientSocket.getOutputStream());
            input = new DataInputStream(clientSocket.getInputStream());
        } catch (IOException e) {
            throw new NetworkException(e);
        }
    }

    public RpcResponse rpcCall(int command, Object... args) {
        try {
            byte[] request = encode(command, seq++, args);

            output.write(request);

            return readResponse(input);
        } catch (IOException e) {
            throw new NetworkException(e);
        }
    }

    private RpcResponse readResponse(DataInputStream inputStream) {
        try {
            byte[] header = new byte[CPROTO_HDR_LEN];
            inputStream.readFully(header);
            ByteBuffer deserializer = new ByteBuffer(header);
            deserializer.rewind();

            long magic = deserializer.readUnsignedInt();
            int version = deserializer.readUnsignedShort();
            deserializer.readUnsignedShort();
            int size = (int) deserializer.readUnsignedInt();
            long rseq = deserializer.readUnsignedShort();

            if (magic != CPROTO_MAGIC) {
                throw new InvalidProtocolException(String.format("Invalid cproto magic '%08X'", magic));
            }

            version &= CPROTO_VERSION_MASK;
            if (version < CPROTO_VERSION) {
                throw new InvalidProtocolException(String.format("Unsupported cproto version '%04X'. " +
                        "This client expects reindexer server v1.9.8+", version));
            }

            byte[] body = new byte[size];
            inputStream.readFully(body);

            deserializer = new ByteBuffer(body);
            deserializer.rewind();
            int code = deserializer.readUnsignedVarInt();
            String message = deserializer.readVString();
            int argsCount = deserializer.readUnsignedVarInt();
            Object[] responseArgs = new Object[argsCount];
            for (int i = 0; i < argsCount; i++) {
                responseArgs[i] = readArgument(deserializer);
            }

            return new RpcResponse(code, message, responseArgs);
        } catch (IOException e) {
            throw new NetworkException(e);
        }
    }

    @Override
    public void close() throws Exception {
        clientSocket.close();
    }

    private Object readArgument(ByteBuffer deserializer) {
        int type = deserializer.readUnsignedVarInt();
        switch (type) {
            case Consts.VALUE_INT:
                return deserializer.readVarInt();
            case Consts.VALUE_BOOL:
                return deserializer.readVarInt() != 0;
            case Consts.VALUE_STRING:
                return deserializer.readVBytes();
            case Consts.VALUE_INT_64:
                return deserializer.readVarLong();
            case Consts.VALUE_DOUBLE:
                return deserializer.readDouble();
            default:
                throw new InvalidProtocolException(String.format("cproto: Unexpected arg type %d", type));
        }
    }

    private byte[] encode(int command, int seq, Object... args) {

        byte[] body = encodeArgs(args);
        byte[] header = encodeHeader(seq, command, body.length);
        byte[] bytes = new byte[header.length + body.length];
        System.arraycopy(header, 0, bytes, 0, header.length);
        System.arraycopy(body, 0, bytes, header.length, body.length);
        return bytes;
    }

    private byte[] encodeArgs(Object[] args) {
        ByteBuffer buffer = new ByteBuffer();
        buffer.writeUnsignedVarInt(args.length);
        for (Object arg : args) {
            if (arg instanceof Boolean) {
                buffer.writeUnsignedVarInt(Consts.VALUE_BOOL)
                        .writeUnsignedVarInt((Boolean) arg ? 1L : 0L);
            } else if (arg instanceof Short) {
                buffer.writeUnsignedVarInt(Consts.VALUE_INT)
                        .writeVarLong(((Short) arg));
            } else if (arg instanceof Integer) {
                buffer.writeUnsignedVarInt(Consts.VALUE_INT)
                        .writeVarLong(((Integer) arg));
            } else if (arg instanceof Long) {
                buffer.writeUnsignedVarInt(Consts.VALUE_INT)
                        .writeVarLong((Long) arg);
            } else if (arg instanceof String) {
                buffer.writeUnsignedVarInt(Consts.VALUE_STRING)
                        .writeString((String) arg);
            } else if (arg instanceof byte[]) {
                buffer.writeUnsignedVarInt(Consts.VALUE_STRING)
                        .write(((byte[]) arg));
            } else {
                throw new IllegalArgumentException("Unsupported data type " + arg.getClass());
            }
        }

        return buffer.bytes();
    }

    private byte[] encodeHeader(int seq, int command, int size) {
        return new ByteBuffer()
                .writeUnsignedInt(CPROTO_MAGIC)
                .writeUnsignedShort(CPROTO_VERSION)
                .writeUnsignedShort(command)
                .writeUnsignedInt(size)
                .writeUnsignedInt(seq)
                .bytes();
    }
}
