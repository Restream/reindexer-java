package ru.rt.restream.reindexer.binding.cproto;

import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.exceptions.InvalidProtocolException;
import ru.rt.restream.reindexer.exceptions.NetworkException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * A connection with a specific reindexer instance. Uses reindexer rpc protocol.
 * Commands are executed and results are returned within the context of a connection.
 */
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

    /**
     * Call a rpc command with specified arguments.
     *
     * @param command command to invoke
     * @param args    command arguments
     * @return rpc call result
     */
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
            ByteBuffer deserializer = new ByteBuffer(header).rewind();

            long magic = deserializer.getUInt32();
            int version = deserializer.getUInt16();
            deserializer.getUInt16();
            int size = (int) deserializer.getUInt32();
            long rseq = deserializer.getUInt16();

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

            deserializer = new ByteBuffer(body).rewind();
            int code = (int) deserializer.getVarUInt();
            String message = deserializer.getVString();
            int argsCount = (int) deserializer.getVarUInt();
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
        int type = (int) deserializer.getVarUInt();
        switch (type) {
            case Consts.VALUE_INT:
                return (int) deserializer.getVarInt();
            case Consts.VALUE_BOOL:
                return deserializer.getVarInt() != 0;
            case Consts.VALUE_STRING:
                return deserializer.getVBytes();
            case Consts.VALUE_INT_64:
                return deserializer.getVarInt();
            case Consts.VALUE_DOUBLE:
                return deserializer.getDouble();
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
        buffer.putVarUInt32(args.length);
        for (Object arg : args) {
            if (arg instanceof Boolean) {
                buffer.putVarUInt32(Consts.VALUE_BOOL)
                        .putVarUInt32((Boolean) arg ? 1L : 0L);
            } else if (arg instanceof Short) {
                buffer.putVarUInt32(Consts.VALUE_INT)
                        .putVarInt64(((Short) arg));
            } else if (arg instanceof Integer) {
                buffer.putVarUInt32(Consts.VALUE_INT)
                        .putVarInt64(((Integer) arg));
            } else if (arg instanceof Long) {
                buffer.putVarUInt32(Consts.VALUE_INT)
                        .putVarInt64((Long) arg);
            } else if (arg instanceof String) {
                buffer.putVarUInt32(Consts.VALUE_STRING)
                        .putVString((String) arg);
            } else if (arg instanceof byte[]) {
                buffer.putVarUInt32(Consts.VALUE_STRING)
                        .putVBytes(((byte[]) arg));
            } else {
                throw new IllegalArgumentException("Unsupported data type " + arg.getClass());
            }
        }

        return buffer.bytes();
    }

    private byte[] encodeHeader(int seq, int command, int size) {
        return new ByteBuffer()
                .putUInt32(CPROTO_MAGIC)
                .putUInt16(CPROTO_VERSION)
                .putUInt16(command)
                .putUInt32(size)
                .putUInt32(seq)
                .bytes();
    }
}
