package ru.rt.restream.reindexer.binding.cproto;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.SneakyThrows;
import ru.rt.restream.reindexer.binding.Binding;
import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.binding.QueryResult;
import ru.rt.restream.reindexer.binding.definition.IndexDefinition;
import ru.rt.restream.reindexer.binding.definition.NamespaceDefinition;
import ru.rt.restream.reindexer.exceptions.ReindexerException;
import ru.rt.restream.reindexer.exceptions.UnimplementedException;

import java.net.URI;

/**
 * A binding to Reindexer database, which establishes a connection to Reindexer instance via RPC.
 */
public class Cproto implements Binding {

    private enum OperationType {
        READ, WRITE
    }

    /**
     * Reindexer instance host.
     */
    private final String host;

    /**
     * Reindexer instance port.
     */
    private final int port;

    /**
     * Name of a database to connect.
     */
    private final String database;

    /**
     * Reindexer users login.
     */
    private final String user;

    /**
     * Reindexer users password.
     */
    private final String password;

    /**
     * Connection with a specific database.
     */
    private final Connection connection;

    /**
     * Construct binding instance to the given database URL.
     *
     * @param url  a database url of the form cproto://host:port/database_name
     * */
    @SneakyThrows
    public Cproto(String url) {
        URI uri = new URI(url);
        host = uri.getHost();
        port = uri.getPort();
        database = uri.getPath().substring(1);
        String userInfo = uri.getUserInfo();
        if (userInfo != null) {
            String[] userInfoArray = userInfo.split(":");
            if (userInfoArray.length == 2) {
                this.user = userInfoArray[0];
                this.password = userInfoArray[1];
            } else {
                throw new IllegalArgumentException();
            }
        } else {
            this.user = "";
            this.password = "";
        }

        connection = new Connection(host, port);
        login();
    }

    private void login() {
        connection.rpcCall(LOGIN, user, password, database);
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
    public long deleteQuery(byte[] queryData) {
        QueryResult queryResult = rpcCallQuery(OperationType.WRITE, DELETE_QUERY, (Object) queryData);
        return queryResult.getCount();
    }

    @Override
    public QueryResult fetchResults(long requestId, boolean asJson, int offset, int limit) {
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

        ByteBuffer buffer = new ByteBuffer(rawQueryResult);
        buffer.rewind();
        long flags = buffer.getVarUInt();
        boolean isJson = (flags & RESULTS_FORMAT_MASK) == RESULT_JSON;

        return QueryResult.builder()
                .requestId(requestId)
                .isJson(isJson)
                .totalCount(buffer.getVarUInt())
                .qCount(buffer.getVarUInt())
                .count(buffer.getVarUInt())
                .buffer(new ByteBuffer(buffer.getBytes()).rewind())
                .build();
    }

    private void rpcCallNoResults(OperationType operationType, int command, Object... args) {
        rpcCall(operationType, command, args);
    }


    private RpcResponse rpcCall(OperationType operationType, int command, Object... args) {
        RpcResponse response = connection.rpcCall(command, args);

        if (response.hasError()) {
            throw new ReindexerException(response.getErrorMessage());
        }

        return response;
    }

}
