package ru.rt.restream.reindexer.binding.cproto;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.rt.restream.reindexer.binding.Binding;
import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.binding.QueryResult;
import ru.rt.restream.reindexer.binding.cproto.cjson.PayloadField;
import ru.rt.restream.reindexer.binding.cproto.cjson.PayloadType;
import ru.rt.restream.reindexer.binding.definition.IndexDefinition;
import ru.rt.restream.reindexer.binding.definition.NamespaceDefinition;
import ru.rt.restream.reindexer.exceptions.ReindexerException;

import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static ru.rt.restream.reindexer.binding.Consts.QUERY_RESULT_END;

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
    public void modifyItem(String namespaceName, int format, byte[] data, int mode, String[] precepts,
                           int stateToken) {
        byte[] packedPercepts = packPrecepts(precepts);
        rpcCallNoResults(OperationType.WRITE, MODIFY_ITEM, namespaceName, format, data, mode,
                packedPercepts, stateToken, 0);
    }

    @Override
    public void modifyItemTx(int format, byte[] data, int mode, String[] precepts, int stateToken, long txId) {
        byte[] packedPrecepts = packPrecepts(precepts);
        rpcCallNoResults(OperationType.WRITE, ADD_TX_ITEM, format, data, mode, packedPrecepts, stateToken, txId);
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
    public QueryResult selectQuery(byte[] queryData, boolean asJson, int fetchCount) {
        int flags;
        if (asJson) {
            flags = Consts.RESULTS_JSON;
        } else {
            flags = Consts.RESULTS_C_JSON | Consts.RESULTS_WITH_PAYLOAD_TYPES | Consts.RESULTS_WITH_ITEM_ID;
        }

        if (fetchCount <= 0) {
            fetchCount = Integer.MAX_VALUE;
        }

        return rpcCallQuery(OperationType.READ, SELECT, queryData, flags, fetchCount, new long[] {1});
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

        return getQueryResult(rawQueryResult, requestId);
    }

    private QueryResult getQueryResult(byte[] rawQueryResult, int requestId) {
        ByteBuffer buffer = new ByteBuffer(rawQueryResult).rewind();
        long flags = buffer.getVarUInt();
        boolean isJson = (flags & RESULTS_FORMAT_MASK) == RESULTS_JSON;
        boolean withRank = (flags & RESULTS_WITH_RANK) != 0;
        boolean withItemId = (flags & RESULTS_WITH_ITEM_ID) != 0;
        boolean withNsId = (flags & RESULTS_WITH_NS_ID) != 0;
        boolean withPayloadTypes = (flags & RESULTS_WITH_PAYLOAD_TYPES) != 0;

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(requestId);
        queryResult.setJson(isJson);
        queryResult.setWithRank(withRank);
        queryResult.setTotalCount(buffer.getVarUInt());
        queryResult.setqCount(buffer.getVarUInt());
        queryResult.setCount(buffer.getVarUInt());
        queryResult.setWithItemId(withItemId);
        queryResult.setWithNsId(withNsId);
        queryResult.setWithPayloadTypes(withPayloadTypes);

        List<PayloadType> payloadTypes = new ArrayList<>();
        queryResult.setPayloadTypes(payloadTypes);
        if (!isJson) {
            if (queryResult.isWithPayloadTypes()) {
                int ptCount = (int) buffer.getVarUInt();
                for (int i = 0; i < ptCount; i++) {
                    long namespaceId = buffer.getVarUInt();
                    String namespaceName = buffer.getVString();
                    long stateToken = buffer.getVarUInt();
                    long version = buffer.getVarUInt();

                    //read tags
                    List<String> tags = new ArrayList<>();
                    long tagsCount = buffer.getVarUInt();
                    for (int j = 0; j < tagsCount; j++) {
                        tags.add(buffer.getVString());
                    }

                    //read payload fields
                    long pStringHdrOffset = buffer.getVarUInt();
                    List<PayloadField> fields = new ArrayList<>();
                    long fieldsCount = buffer.getVarUInt();
                    for (int j = 0; j < fieldsCount; j++) {
                        long type = buffer.getVarUInt();
                        String name = buffer.getVString();
                        long offset = buffer.getVarUInt();
                        long size = buffer.getVarUInt();
                        boolean isArray = buffer.getVarUInt() != 0;
                        long jsonPathCnt = buffer.getVarUInt();
                        List<String> jsonPaths = new ArrayList<>();
                        for (int k = 0; k < jsonPathCnt; k++) {
                            jsonPaths.add(buffer.getVString());
                        }
                        fields.add(new PayloadField(type, name, offset, size, isArray, jsonPaths));
                    }

                    PayloadType payloadType = new PayloadType(namespaceId, namespaceName, version, stateToken,
                            pStringHdrOffset, tags, fields);
                    payloadTypes.add(payloadType);
                }
            }
        }

        readExtraResults(buffer);
        queryResult.setBuffer(new ByteBuffer(buffer.getBytes()).rewind());
        return queryResult;
    }

    private void readExtraResults(ByteBuffer buffer) {
        int tag = (int) buffer.getVarUInt();
        while (tag != QUERY_RESULT_END) {
            byte[] data = buffer.getBytes((int) buffer.getUInt32());
            tag = (int) buffer.getVarUInt();
        }
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
