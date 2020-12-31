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
import ru.rt.restream.reindexer.binding.RequestContext;
import ru.rt.restream.reindexer.binding.TransactionContext;
import ru.rt.restream.reindexer.binding.cproto.util.ConnectionUtils;

import java.util.concurrent.CompletableFuture;

import static ru.rt.restream.reindexer.binding.Binding.SELECT;
import static ru.rt.restream.reindexer.binding.Consts.FORMAT_C_JSON;

/**
 * A transaction context which establish a connection to the Reindexer instance via RPC.
 */
public class CprotoTransactionContext implements TransactionContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(CprotoTransactionContext.class);

    private static final int ADD_TX_ITEM = 26;

    private static final int UPDATE_QUERY_TX = 31;

    private static final int DELETE_QUERY_TX = 30;

    private static final int COMMIT_TX = 27;

    private static final int ROLLBACK_TX = 28;

    private final long transactionId;

    private final Connection connection;

    /**
     * Creates an instance.
     *
     * @param transactionId the transaction id
     * @param connection    the connection in which the transaction is started
     */
    public CprotoTransactionContext(long transactionId, Connection connection) {
        this.transactionId = transactionId;
        this.connection = connection;
    }

    @Override
    public void modifyItem(byte[] data, int mode, String[] precepts, int stateToken) {
        byte[] packedPrecepts = packPrecepts(precepts);
        ConnectionUtils.rpcCallNoResults(connection, ADD_TX_ITEM, FORMAT_C_JSON, data, mode, packedPrecepts, stateToken,
                transactionId);
    }

    @Override
    public CompletableFuture<RpcResponse> modifyItemAsync(byte[] data, int mode, String[] precepts, int stateToken) {
        byte[] packedPrecepts = packPrecepts(precepts);
        return connection.rpcCallAsync(ADD_TX_ITEM, FORMAT_C_JSON, data, mode, packedPrecepts, stateToken, transactionId);
    }

    private byte[] packPrecepts(String[] precepts) {
        byte[] packedPrecepts = new byte[0];
        if (precepts.length > 0) {
            ByteBuffer buffer = new ByteBuffer();
            buffer.putVarUInt32(precepts.length);
            for (String precept : precepts) {
                buffer.putVString(precept);
            }
            packedPrecepts = buffer.bytes();
        }
        return packedPrecepts;
    }

    @Override
    public void updateQuery(byte[] queryData) {
        ConnectionUtils.rpcCallNoResults(connection, UPDATE_QUERY_TX, queryData, transactionId);
    }

    @Override
    public void deleteQuery(byte[] queryData) {
        ConnectionUtils.rpcCallNoResults(connection, DELETE_QUERY_TX, queryData, transactionId);
    }

    @Override
    public RequestContext selectQuery(byte[] queryData, int fetchCount, long[] ptVersions) {
        int flags = Consts.RESULTS_C_JSON | Consts.RESULTS_WITH_PAYLOAD_TYPES | Consts.RESULTS_WITH_ITEM_ID;
        RpcResponse rpcResponse = ConnectionUtils.rpcCall(connection, SELECT, queryData, flags,
                fetchCount > 0 ? fetchCount : Integer.MAX_VALUE, ptVersions);
        return new CprotoRequestContext(rpcResponse, connection);
    }

    @Override
    public void commit() {
        try {
            ConnectionUtils.rpcCallNoResults(connection, COMMIT_TX, transactionId);
        } catch (Exception e) {
            LOGGER.error("rx: commit error", e);
        }
    }

    @Override
    public void rollback() {
        try {
            ConnectionUtils.rpcCallNoResults(connection, ROLLBACK_TX, transactionId);
        } catch (Exception e) {
            LOGGER.error("rx: rollback error", e);
        }
    }

}
