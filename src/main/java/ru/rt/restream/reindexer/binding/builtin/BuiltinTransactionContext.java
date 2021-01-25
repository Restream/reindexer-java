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

package ru.rt.restream.reindexer.binding.builtin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.rt.restream.reindexer.ReindexerResponse;
import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.binding.RequestContext;
import ru.rt.restream.reindexer.binding.TransactionContext;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.exceptions.ReindexerExceptionFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * A transaction context which establish a connection to the Reindexer instance via {@link BuiltinAdapter}.
 */
public class BuiltinTransactionContext implements TransactionContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(BuiltinTransactionContext.class);

    private final BuiltinAdapter adapter;

    private final long rx;

    private final long transactionId;

    private final Supplier<Long> next;

    private final Duration timeout;

    /**
     * Creates an instance.
     *
     * @param adapter       the {@link BuiltinAdapter} to use
     * @param rx            the Reindexer instance pointer
     * @param transactionId the transaction id
     * @param next          the context id generator
     * @param timeout       the execution timeout
     */
    public BuiltinTransactionContext(BuiltinAdapter adapter, long rx, long transactionId,
                                     Supplier<Long> next, Duration timeout) {
        this.adapter = adapter;
        this.rx = rx;
        this.transactionId = transactionId;
        this.next = next;
        this.timeout = timeout;
    }

    @Override
    public void modifyItem(byte[] data, int mode, String[] precepts, int stateToken) {
        ReindexerResponse response = modifyItemInternal(data, mode, precepts, stateToken);
        checkResponse(response);
    }

    @Override
    public CompletableFuture<ReindexerResponse> modifyItemAsync(byte[] data, int mode, String[] precepts, int stateToken) {
        return CompletableFuture.supplyAsync(() -> modifyItemInternal(data, mode, precepts, stateToken));
    }

    private ReindexerResponse modifyItemInternal(byte[] data, int mode, String[] precepts, int stateToken) {
        ByteBuffer args = new ByteBuffer()
                .putVarUInt32(Consts.FORMAT_C_JSON)
                .putVarUInt32(mode)
                .putVarUInt32(stateToken);
        args.putVarUInt32(precepts.length);
        for (String precept : precepts) {
            args.putVString(precept);
        }
        return adapter.modifyItemTx(rx, transactionId, args.bytes(), data);
    }

    @Override
    public RequestContext selectQuery(byte[] queryData, int fetchCount, long[] ptVersions) {
        ReindexerResponse response = adapter.selectQuery(rx, next.get(), timeout.toMillis(), queryData, ptVersions, false);
        checkResponse(response);
        return new BuiltinRequestContext(response);
    }

    @Override
    public void updateQuery(byte[] queryData) {
        ReindexerResponse response = adapter.updateQueryTx(rx, transactionId, queryData);
        checkResponse(response);
    }

    @Override
    public void deleteQuery(byte[] queryData) {
        ReindexerResponse response = adapter.deleteQueryTx(rx, transactionId, queryData);
        checkResponse(response);
    }

    @Override
    public void commit() {
        try {
            ReindexerResponse response = adapter.commitTx(rx, transactionId, next.get(), timeout.toMillis());
            checkResponse(response);
        } catch (Exception e) {
            LOGGER.error("rx: commit error", e);
        }
    }

    @Override
    public void rollback() {
        try {
            ReindexerResponse response = adapter.rollbackTx(rx, transactionId);
            checkResponse(response);
        } catch (Exception e) {
            LOGGER.error("rx: rollback error", e);
        }
    }

    private void checkResponse(ReindexerResponse response) {
        if (response.hasError()) {
            throw ReindexerExceptionFactory.fromResponse(response);
        }
    }

}
