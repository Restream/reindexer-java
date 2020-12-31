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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.exceptions.InvalidProtocolException;
import ru.rt.restream.reindexer.exceptions.NetworkException;
import ru.rt.restream.reindexer.exceptions.ReindexerException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A "Physical" connection with a specific reindexer instance. Uses reindexer rpc protocol.
 * Commands are executed and results are returned within the context of a connection.
 */
public class PhysicalConnection implements Connection {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalConnection.class);

    private static final int QUEUE_SIZE = 512;

    private static final long TASK_DELAY = 0L;

    private static final long TASK_PERIOD = 10L;

    static final long CPROTO_MAGIC = 0xEEDD1132L;

    static final int CPROTO_VERSION = 0x101;

    static final int CPROTO_HDR_LEN = 16;

    static final int CPROTO_VERSION_COMPRESSION_FLAG = 1 << 10;

    static final int CPROTO_VERSION_MASK = 0x3FF;

    private final AtomicLong seq = new AtomicLong(0L);

    private final BlockingQueue<byte[]> requests = new ArrayBlockingQueue<>(QUEUE_SIZE);

    private final Map<Long, BlockingQueue<RpcResponse>> responses = new ConcurrentHashMap<>();

    private final List<Future<?>> futures = new ArrayList<>();

    private final Socket clientSocket;

    private final DataOutputStream output;

    private final DataInputStream input;

    private final long requestTimeout;

    public PhysicalConnection(String host, int port, long requestTimeout, ScheduledExecutorService scheduler) {
        try {
            clientSocket = new Socket(host, port);
            output = new DataOutputStream(clientSocket.getOutputStream());
            input = new DataInputStream(clientSocket.getInputStream());
            this.requestTimeout = requestTimeout;
            scheduleReadTask(scheduler);
            scheduleWriteTask(scheduler);
        } catch (IOException e) {
            throw new NetworkException(e);
        }
    }

    private void scheduleReadTask(ScheduledExecutorService scheduler) {
        ScheduledFuture<?> future = scheduler.scheduleWithFixedDelay(() -> {
            try {
                RpcResponse rpcResponse = readResponse(input);
                long seqNum = rpcResponse.getSeqNum();
                BlockingQueue<RpcResponse> output = responses.get(seqNum);
                if (output != null) {
                    output.add(rpcResponse);
                }
            } catch (EOFException e) {
                // expected
            } catch (IOException e) {
                throw new ReindexerException(e);
            } catch (Exception e) {
                LOGGER.error("rx: read task error", e);
            }
        }, TASK_DELAY, TASK_PERIOD, TimeUnit.MILLISECONDS);
        futures.add(future);
    }

    private void scheduleWriteTask(ScheduledExecutorService scheduler) {
        ScheduledFuture<?> future = scheduler.scheduleWithFixedDelay(() -> {
            try {
                byte[] request = requests.take();
                output.write(request);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                throw new ReindexerException(e);
            } catch (Exception e) {
                LOGGER.error("rx: write task error", e);
            }
        }, TASK_DELAY, TASK_PERIOD, TimeUnit.MILLISECONDS);
        futures.add(future);
    }

    /**
     * Call a rpc command with specified arguments.
     *
     * @param command command to invoke
     * @param args    command arguments
     * @return rpc call result
     */
    @Override
    public RpcResponse rpcCall(int command, Object... args) {
        long seqNum = seq.getAndIncrement();
        responses.put(seqNum, new ArrayBlockingQueue<>(1));
        try {
            byte[] request = encode(command, seqNum, args);
            if (!requests.offer(request, requestTimeout, TimeUnit.SECONDS)) {
                throw new ReindexerException("Request queue is full");
            }
            RpcResponse rpcResponse = responses.get(seqNum).poll(requestTimeout, TimeUnit.SECONDS);
            if (rpcResponse == null) {
                throw new ReindexerException("Request timeout");
            }
            return rpcResponse;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ReindexerException("Interrupted while rpcCall", e);
        } finally {
            responses.remove(seqNum);
        }
    }

    private RpcResponse readResponse(DataInputStream inputStream) throws IOException {
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

        return new RpcResponse(code, rseq, message, responseArgs);
    }

    @Override
    public void close() {
        futures.forEach(f -> f.cancel(true));
        try {
            clientSocket.close();
        } catch (IOException e) {
            LOGGER.error("rx: connection close error", e);
        }
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

    private byte[] encode(int command, long seq, Object... args) {

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
                buffer.putVarUInt32(Consts.VALUE_INT_64)
                        .putVarInt64((Long) arg);
            } else if (arg instanceof String) {
                buffer.putVarUInt32(Consts.VALUE_STRING)
                        .putVString((String) arg);
            } else if (arg instanceof byte[]) {
                buffer.putVarUInt32(Consts.VALUE_STRING)
                        .putVBytes(((byte[]) arg));
            } else if (arg instanceof long[]) {
                long[] array = (long[]) arg;
                buffer.putVarUInt32(Consts.VALUE_STRING);
                ByteBuffer arrayBuffer = new ByteBuffer();
                arrayBuffer.putVarUInt32(array.length);
                for (long i : array) {
                    arrayBuffer.putVarUInt32(i);
                }
                buffer.putVBytes(arrayBuffer.bytes());
            } else {
                throw new IllegalArgumentException("Unsupported data type " + arg.getClass());
            }
        }

        return buffer.bytes();
    }

    private byte[] encodeHeader(long seq, int command, int size) {
        return new ByteBuffer()
                .putUInt32(CPROTO_MAGIC)
                .putUInt16(CPROTO_VERSION)
                .putUInt16(command)
                .putUInt32(size)
                .putUInt32(seq)
                .bytes();
    }
}
