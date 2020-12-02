package ru.rt.restream.reindexer.binding.cproto;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.rt.restream.reindexer.binding.Binding;
import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.binding.QueryResult;
import ru.rt.restream.reindexer.binding.definition.IndexDefinition;
import ru.rt.restream.reindexer.binding.definition.NamespaceDefinition;
import ru.rt.restream.reindexer.exceptions.ReindexerException;
import ru.rt.restream.reindexer.exceptions.UnimplementedException;

/**
 * A binding to Reindexer database, which establishes a connection to Reindexer instance via RPC.
 */
public class Cproto implements Binding {

    private static final Logger LOGGER = LoggerFactory.getLogger(Cproto.class);

    private enum OperationType {
        READ, WRITE
    }

    /**
     * The connection pool.
     */
    private final ConnectionPool pool;

    /**
     * Construct binding instance to the given database URL.
     *
     * @param url  a database url of the form cproto://host:port/database_name
     * @param connectionPoolSize the connection pool size
     * @param connectionTimeout  the connection timeout
     * */
    public Cproto(String url, int connectionPoolSize, long connectionTimeout) {
        pool = new ConnectionPool(url, connectionPoolSize, connectionTimeout);
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
    public void modifyItem(String namespaceName, int format, byte[] data, int mode, String[] percepts,
                           int stateToken) {
        byte[] packedPercepts = new byte[0];
        if (percepts.length > 0) {
            throw new UnimplementedException();
        }
        rpcCallNoResults(OperationType.WRITE, MODIFY_ITEM, namespaceName, format, data, mode,
                packedPercepts, stateToken, 0);
    }

    @Override
    public void modifyItemTx(int format, byte[] data, int mode, String[] percepts, int stateToken, long txId) {
        byte[] packedPercepts = new byte[0];
        rpcCallNoResults(OperationType.WRITE, ADD_TX_ITEM, format, data, mode, packedPercepts, stateToken, txId);
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
    public QueryResult selectQuery(byte[] queryData, boolean asJson, int fetchCount) {
        int flags = 0;
        if (asJson) {
            flags = Consts.RESULTS_JSON;
        } else {
            flags |= Consts.RESULTS_C_JSON | Consts.RESULTS_WITH_PAYLOAD_TYPES | Consts.RESULTS_WITH_ITEM_ID;
            throw new UnimplementedException();
        }

        if (fetchCount <= 0) {
            fetchCount = Integer.MAX_VALUE;
        }

        return rpcCallQuery(OperationType.READ, SELECT, queryData, flags, fetchCount, new byte[]{0});
    }

    @Override
    public void deleteQuery(byte[] queryData) {
        rpcCallNoResults(OperationType.WRITE, DELETE_QUERY, queryData);
    }

    @Override
    public void deleteQueryTx(byte[] queryData, long txId) {
        rpcCallNoResults(OperationType.WRITE, DELETE_QUERY_TX, queryData, txId);
    }

    @Override
    public void updateQuery(byte[] queryData) {
        rpcCallNoResults(OperationType.WRITE, UPDATE_QUERY, queryData);
    }

    @Override
    public void updateQueryTx(byte[] queryData, long txId) {
        rpcCallNoResults(OperationType.WRITE, UPDATE_QUERY_TX, queryData, txId);
    }

    @Override
    public QueryResult fetchResults(int requestId, boolean asJson, int offset, int limit) {
        int flags = 0;
        if (asJson) {
            flags = Consts.RESULTS_JSON;
        } else {
            flags |= Consts.RESULTS_C_JSON | Consts.RESULTS_WITH_PAYLOAD_TYPES | Consts.RESULTS_WITH_ITEM_ID;
            throw new UnimplementedException();
        }

        int fetchCount = limit <= 0 ? Integer.MAX_VALUE : limit;

        return rpcCallQuery(OperationType.READ, FETCH_RESULTS, requestId, flags, offset, fetchCount);
    }

    @Override
    public long beginTx(String namespaceName) {
        RpcResponse rpcResponse = rpcCall(OperationType.WRITE, START_TRANSACTION, namespaceName);
        Object[] responseArguments = rpcResponse.getArguments();
        return responseArguments.length > 0 ? (long) responseArguments[0] : -1L;
    }

    @Override
    public long commitTx(long txId) {
        QueryResult queryResult = rpcCallQuery(OperationType.WRITE, COMMIT_TX, txId);
        return queryResult.getCount();
    }

    @Override
    public void rollback(long txId) {
        rpcCallNoResults(OperationType.WRITE, ROLLBACK_TX, txId);
    }

    @Override
    public void closeResults(long requestId) {
        RpcResponse rpcResponse = rpcCall(CLOSE_RESULTS, requestId);
        if (rpcResponse.hasError()) {
            LOGGER.error("rx: query close error: {}", rpcResponse.getErrorMessage());
        }
    }

    /**
     * Closes the connection pool.
     */
    @Override
    public void close() {
        pool.close();
    }

    private QueryResult rpcCallQuery(OperationType operationType, int command, Object... args) {
        RpcResponse rpcResponse = rpcCall(operationType, command, args);

        byte[] rawQueryResult = new byte[0];
        int requestId = -1;
        Object[] responseArguments = rpcResponse.getArguments();
        if (responseArguments.length > 0) {
            rawQueryResult = (byte[]) responseArguments[0];
        }
        if (responseArguments.length > 1) {
            requestId = (int) responseArguments[1];
        }

        ByteBuffer buffer = new ByteBuffer(rawQueryResult).rewind();
        long flags = buffer.getVarUInt();
        boolean isJson = (flags & RESULTS_FORMAT_MASK) == RESULTS_JSON;
        boolean withRank = (flags & RESULTS_WITH_RANK) == 64;

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(requestId);
        queryResult.setJson(isJson);
        queryResult.setWithRank(withRank);
        queryResult.setTotalCount(buffer.getVarUInt());
        queryResult.setqCount(buffer.getVarUInt());
        queryResult.setCount(buffer.getVarUInt());
        queryResult.setBuffer(new ByteBuffer(buffer.getBytes()).rewind());

        return queryResult;
    }

    private void rpcCallNoResults(OperationType operationType, int command, Object... args) {
        rpcCall(operationType, command, args);
    }


    private RpcResponse rpcCall(OperationType operationType, int command, Object... args) {
        RpcResponse response = rpcCall(command, args);
        if (response.hasError()) {
            throw new ReindexerException(response.getErrorMessage());
        }

        return response;
    }

    private RpcResponse rpcCall(int command, Object... args) {
        try (Connection connection = pool.getConnection()) {
            return connection.rpcCall(command, args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
