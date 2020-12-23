/**
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

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import ru.rt.restream.reindexer.binding.Binding;
import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.binding.RequestContext;
import ru.rt.restream.reindexer.binding.TransactionContext;
import ru.rt.restream.reindexer.binding.cproto.util.ConnectionUtils;
import ru.rt.restream.reindexer.binding.definition.IndexDefinition;
import ru.rt.restream.reindexer.binding.definition.NamespaceDefinition;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;

/**
 * A binding to Reindexer database, which establishes a connection to Reindexer instance via RPC.
 */
public class Cproto implements Binding {

    private enum OperationType {
        READ, WRITE
    }

    /**
     * The connection pool executor.
     */
    private final ConnectionPoolExecutor executor;

    /**
     * Construct binding instance to the given database URL.
     *
     * @param url                a database url of the form cproto://host:port/database_name
     * @param connectionPoolSize the connection pool size
     * @param connectionTimeout  the connection timeout
     */
    public Cproto(String url, int connectionPoolSize, long connectionTimeout) {
        ConnectionPool pool = new ConnectionPool(url, connectionPoolSize, connectionTimeout);
        executor = new ConnectionPoolExecutor(pool);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void openNamespace(NamespaceDefinition namespace) {
        String json = toJson(namespace);
        rpcCallNoResults(OperationType.WRITE, OPEN_NAMESPACE, json);
    }

    private String toJson(Object object) {
        Gson gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .create();
        return gson.toJson(object);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addIndex(String namespace, IndexDefinition index) {
        rpcCallNoResults(OperationType.WRITE, ADD_INDEX, namespace, toJson(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void modifyItem(String namespaceName, int format, byte[] data, int mode, String[] precepts,
                           int stateToken) {
        byte[] packedPercepts = packPrecepts(precepts);
        rpcCallNoResults(OperationType.WRITE, MODIFY_ITEM, namespaceName, format, data, mode,
                packedPercepts, stateToken, 0);
    }

    private byte[] packPrecepts(String[] precepts) {
        if (precepts.length == 0) {
            return EMPTY_BYTE_ARRAY;
        }
        ByteBuffer buffer = new ByteBuffer();
        buffer.putVarUInt32(precepts.length);
        for (String precept : precepts) {
            buffer.putVString(precept);
        }
        return buffer.bytes();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void dropNamespace(String namespaceName) {
        rpcCallNoResults(OperationType.WRITE, DROP_NAMESPACE, namespaceName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void closeNamespace(String namespaceName) {
        rpcCallNoResults(OperationType.WRITE, CLOSE_NAMESPACE, namespaceName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RequestContext selectQuery(byte[] queryData, boolean asJson, int fetchCount) {
        int flags;
        if (asJson) {
            flags = Consts.RESULTS_JSON;
        } else {
            flags = Consts.RESULTS_C_JSON | Consts.RESULTS_WITH_PAYLOAD_TYPES | Consts.RESULTS_WITH_ITEM_ID;
        }
        return executor.executeInConnection(conn -> {
            RpcResponse rpcResponse = ConnectionUtils.rpcCall(conn, SELECT, queryData, flags,
                    fetchCount > 0 ? fetchCount : Integer.MAX_VALUE, new long[]{1});
            return new CprotoRequestContext(rpcResponse, conn, false);
        }, true);
    }

    @Override
    public void deleteQuery(byte[] queryData) {
        rpcCallNoResults(OperationType.WRITE, DELETE_QUERY, queryData);
    }

    @Override
    public void updateQuery(byte[] queryData) {
        rpcCallNoResults(OperationType.WRITE, UPDATE_QUERY, queryData);
    }

    @Override
    public TransactionContext beginTx(String namespaceName) {
        return executor.executeInConnection(conn -> {
            RpcResponse rpcResponse = ConnectionUtils.rpcCall(conn, START_TRANSACTION, namespaceName);
            Object[] responseArguments = rpcResponse.getArguments();
            long transactionId = responseArguments.length > 0 ? (long) responseArguments[0] : -1L;
            return new CprotoTransactionContext(transactionId, conn);
        }, true);
    }

    /**
     * Closes the connection pool executor.
     */
    @Override
    public void close() {
        executor.close();
    }

    private void rpcCallNoResults(OperationType operationType, int command, Object... args) {
        executor.executeInConnection(conn -> {
            ConnectionUtils.rpcCallNoResults(conn, command, args);
            return null;
        }, false);
    }

}
