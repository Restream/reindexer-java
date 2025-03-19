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
import ru.rt.restream.reindexer.ReindexerResponse;
import ru.rt.restream.reindexer.binding.Binding;
import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.binding.cproto.util.ConnectionUtils;
import ru.rt.restream.reindexer.exceptions.InvalidProtocolException;
import ru.rt.restream.reindexer.exceptions.NetworkException;
import ru.rt.restream.reindexer.exceptions.ReindexerException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static ru.rt.restream.reindexer.binding.Consts.APP_PROPERTY_NAME;
import static ru.rt.restream.reindexer.binding.Consts.BINDING_CAPABILITY_COMPLEX_RANK;
import static ru.rt.restream.reindexer.binding.Consts.BINDING_CAPABILITY_RESULTS_WITH_SHARD_IDS;
import static ru.rt.restream.reindexer.binding.Consts.DEF_APP_NAME;
import static ru.rt.restream.reindexer.binding.Consts.REINDEXER_VERSION;

/**
 * A "Physical" connection with a specific reindexer instance. Uses reindexer rpc protocol.
 * Commands are executed and results are returned within the context of a connection.
 */
public class PhysicalConnection implements Connection {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalConnection.class);

    private static final int BUFFER_CAPACITY = 16 * 1024;

    private static final int QUEUE_SIZE = 512;

    private static final int MAX_SEQ_NUM = QUEUE_SIZE * 1000000;

    static final long CPROTO_MAGIC = 0xEEDD1132L;

    static final int CPROTO_VERSION = 0x104;

    static final int CPROTO_HDR_LEN = 16;

    static final int CPROTO_VERSION_COMPRESSION_FLAG = 1 << 10;

    static final int CPROTO_VERSION_MASK = 0x3FF;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Condition notEmptyBuffer = lock.writeLock().newCondition();

    private ByteBuffer headBuffer = new ByteBuffer(BUFFER_CAPACITY);

    private ByteBuffer tailBuffer = new ByteBuffer(BUFFER_CAPACITY);

    private Exception error;

    private final Socket clientSocket;

    private final DataOutputStream output;

    private final DataInputStream input;

    private final Duration timeout;

    private final ScheduledExecutorService scheduler;

    private final BlockingQueue<Integer> sequences = new ArrayBlockingQueue<>(QUEUE_SIZE);

    private final List<RpcRequest> requests = new ArrayList<>(QUEUE_SIZE);

    private final ScheduledFuture<?> readTaskFuture;

    private final ScheduledFuture<?> writeTaskFuture;

    public PhysicalConnection(String host, int port, String user, String password, String database,
                              Duration requestTimeout, ScheduledExecutorService scheduler) {
        try {
            clientSocket = new Socket(host, port);
            output = new DataOutputStream(clientSocket.getOutputStream());
            input = new DataInputStream(clientSocket.getInputStream());
            timeout = requestTimeout;
            this.scheduler = scheduler;
            for (int i = 0; i < QUEUE_SIZE; i++) {
                requests.add(new RpcRequest());
                sequences.add(i);
            }
            readTaskFuture = scheduler.scheduleWithFixedDelay(new ReadTask(), 0, 100, TimeUnit.MICROSECONDS);
            writeTaskFuture = scheduler.scheduleWithFixedDelay(new WriteTask(), 0, 100, TimeUnit.MICROSECONDS);
            ConnectionUtils.rpcCallNoResults(this, Binding.LOGIN, user, password, database,
                    false, // create DB if missing
                    false, // checkClusterID
                    -1,    // expectedClusterID
                    REINDEXER_VERSION,
                    getAppName(),
                    BINDING_CAPABILITY_RESULTS_WITH_SHARD_IDS | BINDING_CAPABILITY_COMPLEX_RANK);
        } catch (Exception e) {
            onError(e);
            throw new NetworkException(e);
        }
    }

    private Object getAppName() {
        return System.getProperty(APP_PROPERTY_NAME, DEF_APP_NAME);
    }

    /**
     * Call a rpc command with specified arguments.
     *
     * @param command command to invoke
     * @param args    command arguments
     * @return rpc call result
     */
    @Override
    public ReindexerResponse rpcCall(int command, Object... args) {
        Exception error = getCurrentError();
        if (error != null) {
            throw new ReindexerException(error);
        }
        try {
            Sequence seq = awaitSeqNum();
            int reqId = seq.seqNum % QUEUE_SIZE;
            RpcRequest rpcRequest = requests.get(reqId);
            rpcRequest.seqNum = seq.seqNum;
            try {
                write(command, seq.seqNum, args);
                for (; ; ) {
                    BufferedResponse bufferedResponse = rpcRequest.reply.poll(seq.timeout.toMillis(), TimeUnit.MILLISECONDS);
                    if (bufferedResponse == null) {
                        throw new ReindexerException("Request timeout");
                    }
                    if (bufferedResponse.seqNum == seq.seqNum) {
                        return readResponse(bufferedResponse.buffer);
                    }
                }
            } finally {
                rpcRequest.seqNum = MAX_SEQ_NUM;
                sequences.add(nextSeqNum(seq.seqNum));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ReindexerException("Interrupted while rpcCall", e);
        }
    }

    private ReindexerResponse readResponse(ByteBuffer deserializer) {
        int code = (int) deserializer.getVarUInt();
        String message = deserializer.getVString();
        int argsCount = (int) deserializer.getVarUInt();
        Object[] responseArgs = new Object[argsCount];
        for (int i = 0; i < argsCount; i++) {
            responseArgs[i] = readArgument(deserializer);
        }
        return new ReindexerResponse(code, message, responseArgs);
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

    @Override
    public CompletableFuture<ReindexerResponse> rpcCallAsync(int command, Object... args) {
        CompletableFuture<ReindexerResponse> completion = new CompletableFuture<>();
        Exception error = getCurrentError();
        if (error != null) {
            completion.completeExceptionally(error);
            return completion;
        }
        try {
            Sequence seq = awaitSeqNum();
            int reqId = seq.seqNum % QUEUE_SIZE;
            RpcRequest rpcRequest = requests.get(reqId);
            rpcRequest.completionLock.lock();
            try {
                rpcRequest.completion = completion;
                rpcRequest.seqNum = seq.seqNum;
                rpcRequest.isAsync = true;
                rpcRequest.timeoutTaskFuture = scheduler.schedule(new TimeoutTask(seq.seqNum),
                        seq.timeout.toMillis(), TimeUnit.MILLISECONDS);
            } finally {
                rpcRequest.completionLock.unlock();
            }
            write(command, seq.seqNum, args);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            completion.completeExceptionally(e);
        } catch (Exception e) {
            completion.completeExceptionally(e);
        }
        return completion;
    }

    private Sequence awaitSeqNum() throws InterruptedException {
        Instant start = Instant.now();
        Integer seqNum = sequences.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (seqNum == null) {
            throw new ReindexerException("Request queue is full");
        }
        Duration remainingTimeout = timeout.minus(Duration.between(start, Instant.now()));
        if (remainingTimeout.isNegative() || remainingTimeout.isZero()) {
            sequences.add(seqNum);
            throw new ReindexerException("Request timeout");
        }
        return new Sequence(seqNum, remainingTimeout);
    }

    private void write(int command, int seqNum, Object[] args) {
        lock.writeLock().lock();
        try {
            headBuffer.writeBytes(encode(command, seqNum, args));
            notEmptyBuffer.signalAll();
        } finally {
            lock.writeLock().unlock();
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
                    arrayBuffer.putVarInt64(i);
                }
                buffer.putVBytes(arrayBuffer.bytes());
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

    private int nextSeqNum(int seqNum) {
        int result = seqNum + QUEUE_SIZE;
        if (isSeqNumValid(result)) {
            return result;
        }
        return result - MAX_SEQ_NUM;
    }

    private boolean isSeqNumValid(int seqNum) {
        return seqNum < MAX_SEQ_NUM;
    }

    @Override
    public boolean hasError() {
        return getCurrentError() != null;
    }

    private Exception getCurrentError() {
        lock.readLock().lock();
        try {
            return error;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void onError(Exception error) {
        lock.writeLock().lock();
        try {
            if (this.error == null) {
                this.error = error;
                close();
                for (RpcRequest rpcRequest : requests) {
                    if (rpcRequest.isAsync) {
                        CompletableFuture<ReindexerResponse> completion = null;
                        ScheduledFuture<?> timeoutTaskFuture = null;
                        Integer seqNum = null;
                        rpcRequest.completionLock.lock();
                        try {
                            if (rpcRequest.completion != null) {
                                completion = rpcRequest.completion;
                                rpcRequest.completion = null;
                                timeoutTaskFuture = rpcRequest.timeoutTaskFuture;
                                rpcRequest.timeoutTaskFuture = null;
                                seqNum = rpcRequest.seqNum;
                                rpcRequest.seqNum = MAX_SEQ_NUM;
                                rpcRequest.isAsync = false;
                            }
                        } finally {
                            rpcRequest.completionLock.unlock();
                        }
                        if (seqNum != null) {
                            sequences.add(nextSeqNum(seqNum));
                        }
                        if (completion != null) {
                            completion.completeExceptionally(error);
                        }
                        if (timeoutTaskFuture != null) {
                            timeoutTaskFuture.cancel(true);
                        }
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        if (readTaskFuture != null) {
            readTaskFuture.cancel(true);
        }
        if (writeTaskFuture != null) {
            writeTaskFuture.cancel(true);
        }
        if (clientSocket != null) {
            try {
                clientSocket.close();
            } catch (IOException e) {
                LOGGER.error("rx: connection close error", e);
            }
        }
    }

    private static class Sequence {

        private final int seqNum;

        private final Duration timeout;

        private Sequence(int seqNum, Duration timeout) {
            this.seqNum = seqNum;
            this.timeout = timeout;
        }

    }

    private static class RpcRequest {

        private volatile int seqNum;

        private volatile boolean isAsync;

        private final BlockingQueue<BufferedResponse> reply = new LinkedBlockingQueue<>();

        private final Lock completionLock = new ReentrantLock();

        private CompletableFuture<ReindexerResponse> completion;

        private ScheduledFuture<?> timeoutTaskFuture;

    }

    private static class BufferedResponse {

        private final int seqNum;

        private final ByteBuffer buffer;

        private BufferedResponse(int seqNum, ByteBuffer buffer) {
            this.seqNum = seqNum;
            this.buffer = buffer;
        }

    }

    private class ReadTask implements Runnable {

        @Override
        public void run() {
            try {
                byte[] header = new byte[CPROTO_HDR_LEN];
                input.readFully(header);
                ByteBuffer deserializer = new ByteBuffer(header).rewind();
                long magic = deserializer.getUInt32();
                if (magic != CPROTO_MAGIC) {
                    throw new InvalidProtocolException(String.format("Invalid cproto magic '%08X'", magic));
                }
                int version = deserializer.getUInt16();
                deserializer.getUInt16();
                int size = (int) deserializer.getUInt32();
                int rseq = (int) deserializer.getUInt32();
                version &= CPROTO_VERSION_MASK;
                if (version < CPROTO_VERSION) {
                    throw new InvalidProtocolException(String.format("Unsupported cproto version '%04X'. " +
                                                                     "This client expects reindexer server v1.9.8+", version));
                }
                if (!isSeqNumValid(rseq)) {
                    throw new InvalidProtocolException(String.format("Invalid seq num: %d", rseq));
                }
                int reqId = rseq % QUEUE_SIZE;
                RpcRequest rpcRequest = requests.get(reqId);
                if (rpcRequest.seqNum != rseq) {
                    input.skipBytes(size);
                    return;
                }
                byte[] body = new byte[size];
                input.readFully(body);
                deserializer = new ByteBuffer(body).rewind();
                if (rpcRequest.isAsync) {
                    CompletableFuture<ReindexerResponse> completion = null;
                    ScheduledFuture<?> timeoutTaskFuture = null;
                    Integer seqNum = null;
                    rpcRequest.completionLock.lock();
                    try {
                        if (rpcRequest.completion != null && rpcRequest.seqNum == rseq) {
                            completion = rpcRequest.completion;
                            rpcRequest.completion = null;
                            timeoutTaskFuture = rpcRequest.timeoutTaskFuture;
                            rpcRequest.timeoutTaskFuture = null;
                            seqNum = rpcRequest.seqNum;
                            rpcRequest.seqNum = MAX_SEQ_NUM;
                            rpcRequest.isAsync = false;
                        }
                    } finally {
                        rpcRequest.completionLock.unlock();
                    }
                    if (seqNum != null) {
                        sequences.add(nextSeqNum(seqNum));
                    }
                    if (completion != null) {
                        completion.complete(readResponse(deserializer));
                    }
                    if (timeoutTaskFuture != null) {
                        timeoutTaskFuture.cancel(true);
                    }
                } else {
                    rpcRequest.reply.add(new BufferedResponse(rseq, deserializer));
                }
            } catch (Exception e) {
                onError(e);
            }
        }

    }

    private class WriteTask implements Runnable {

        @Override
        public void run() {
            try {
                lock.writeLock().lock();
                try {
                    while (headBuffer.length() == 0) {
                        notEmptyBuffer.await();
                    }
                    ByteBuffer head = headBuffer;
                    headBuffer = tailBuffer;
                    tailBuffer = head;
                } finally {
                    lock.writeLock().unlock();
                }
                output.write(tailBuffer.bytes());
                tailBuffer.reset();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                onError(e);
            } catch (Exception e) {
                onError(e);
            }
        }

    }

    private class TimeoutTask implements Runnable {

        private final int rseq;

        private TimeoutTask(int rseq) {
            this.rseq = rseq;
        }

        @Override
        public void run() {
            int reqId = rseq % QUEUE_SIZE;
            RpcRequest rpcRequest = requests.get(reqId);
            if (rpcRequest.isAsync) {
                CompletableFuture<ReindexerResponse> completion = null;
                Integer seqNum = null;
                rpcRequest.completionLock.lock();
                try {
                    if (rpcRequest.completion != null && rpcRequest.seqNum == rseq) {
                        completion = rpcRequest.completion;
                        rpcRequest.completion = null;
                        rpcRequest.timeoutTaskFuture = null;
                        seqNum = rpcRequest.seqNum;
                        rpcRequest.seqNum = MAX_SEQ_NUM;
                        rpcRequest.isAsync = false;
                    }
                } finally {
                    rpcRequest.completionLock.unlock();
                }
                if (seqNum != null) {
                    sequences.add(nextSeqNum(seqNum));
                }
                if (completion != null) {
                    completion.completeExceptionally(new ReindexerException("Request timeout"));
                }
            }
        }

    }

}
