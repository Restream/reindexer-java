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

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import ru.rt.restream.reindexer.ReindexerResponse;
import ru.rt.restream.reindexer.binding.Binding;
import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.binding.RequestContext;
import ru.rt.restream.reindexer.binding.TransactionContext;
import ru.rt.restream.reindexer.binding.cproto.util.ConnectionUtils;
import ru.rt.restream.reindexer.binding.definition.IndexDefinition;
import ru.rt.restream.reindexer.binding.definition.NamespaceDefinition;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;

/**
 * A binding to Reindexer database, which establishes a connection to Reindexer instance via RPC.
 */
public class Cproto implements Binding {

    /**
     * The connection pool.
     */
    private final ConnectionPool pool;

    /**
     * Construct binding instance to the given database URL.
     *
     * @param dataSourceFactory  the {@link DataSourceFactory} to use
     * @param dataSourceConfig   the {@link DataSourceConfiguration} to configure an obtaining of {@link DataSource}
     * @param connectionPoolSize the connection pool size
     * @param requestTimeout     the request timeout
     */
    public Cproto(DataSourceFactory dataSourceFactory, DataSourceConfiguration dataSourceConfig, int connectionPoolSize,
                  Duration requestTimeout) {
        pool = new ConnectionPool(dataSourceFactory, dataSourceConfig, connectionPoolSize, requestTimeout);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void openNamespace(NamespaceDefinition namespace) {
        String json = toJson(namespace);
        rpcCallNoResults(OPEN_NAMESPACE, json);
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
        rpcCallNoResults(ADD_INDEX, namespace, toJson(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateIndex(String namespace, IndexDefinition index) {
        rpcCallNoResults(UPDATE_INDEX, namespace, toJson(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void dropIndex(String namespace, String indexName) {
        rpcCallNoResults(DROP_INDEX, namespace, indexName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void modifyItem(String namespaceName, byte[] data, int format, int mode, String[] precepts,
                           int stateToken) {
        byte[] packedPercepts = packPrecepts(precepts);
        rpcCallNoResults(MODIFY_ITEM, namespaceName, format, data, mode,
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
        rpcCallNoResults(DROP_NAMESPACE, namespaceName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void closeNamespace(String namespaceName) {
        rpcCallNoResults(CLOSE_NAMESPACE, namespaceName);
    }

    @Override
    public RequestContext select(String query, boolean asJson, int fetchCount, long[] ptVersions) {
        int flags = asJson
                ? Consts.RESULTS_JSON
                : Consts.RESULTS_C_JSON | Consts.RESULTS_WITH_PAYLOAD_TYPES;
        Connection connection = pool.getConnection();
        ReindexerResponse rpcResponse = ConnectionUtils.rpcCall(connection, SELECT_SQL, query, flags,
                fetchCount > 0 ? fetchCount : Integer.MAX_VALUE, ptVersions);
        return new CprotoRequestContext(rpcResponse, connection, asJson);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RequestContext selectQuery(byte[] queryData, int fetchCount, long[] ptVersions, boolean asJson) {
        int flags = asJson
                ? Consts.RESULTS_JSON
                : Consts.RESULTS_C_JSON | Consts.RESULTS_WITH_PAYLOAD_TYPES;
        Connection connection = pool.getConnection();
        ReindexerResponse rpcResponse = ConnectionUtils.rpcCall(connection, SELECT, queryData, flags,
                fetchCount > 0 ? fetchCount : Integer.MAX_VALUE, ptVersions);
        return new CprotoRequestContext(rpcResponse, connection, asJson);
    }

    @Override
    public void deleteQuery(byte[] queryData) {
        rpcCallNoResults(DELETE_QUERY, queryData);
    }

    @Override
    public void updateQuery(byte[] queryData) {
        rpcCallNoResults(UPDATE_QUERY, queryData);
    }

    @Override
    public TransactionContext beginTx(String namespaceName) {
        Connection connection = pool.getConnection();
        ReindexerResponse rpcResponse = ConnectionUtils.rpcCall(connection, START_TRANSACTION, namespaceName);
        Object[] responseArguments = rpcResponse.getArguments();
        long transactionId = responseArguments.length > 0 ? (long) responseArguments[0] : -1L;
        return new CprotoTransactionContext(transactionId, connection);
    }

    @Override
    public void putMeta(String namespace, String key, String data) {
        rpcCallNoResults(PUT_META, namespace, key, data);
    }

    @Override
    public String getMeta(String namespace, String key) {
        Connection connection = pool.getConnection();
        ReindexerResponse response = ConnectionUtils.rpcCall(connection, GET_META, namespace, key);
        return new String((byte[]) response.getArguments()[0], StandardCharsets.UTF_8);
    }

    /**
     * Closes the connection pool.
     */
    @Override
    public void close() {
        pool.close();
    }

    private void rpcCallNoResults(int command, Object... args) {
        Connection connection = pool.getConnection();
        ConnectionUtils.rpcCallNoResults(connection, command, args);
    }

}
