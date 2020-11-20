package ru.rt.restream.reindexer.connector.binding.cproto;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import ru.rt.restream.reindexer.connector.StorageOpts;
import ru.rt.restream.reindexer.connector.binding.Binding;
import ru.rt.restream.reindexer.connector.binding.Consts;
import ru.rt.restream.reindexer.connector.binding.QueryResult;
import ru.rt.restream.reindexer.connector.binding.def.IndexDef;
import ru.rt.restream.reindexer.connector.binding.def.NamespaceDef;
import ru.rt.restream.reindexer.connector.exceptions.ReindexerException;
import ru.rt.restream.reindexer.connector.exceptions.UnimplementedException;

import java.net.URI;
import java.net.URISyntaxException;

public class Cproto implements Binding {

    private enum OperationType {
        READ, WRITE
    }

    private final String host;
    private final int port;
    private final String database;
    private final String user;
    private final String password;

    private final Connection connection;

    public Cproto(String url) {
        URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        this.host = uri.getHost();
        this.port = uri.getPort();
        this.database = uri.getPath().substring(1);
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

    @Override
    public void openNamespace(String namespace, boolean enableStorage, boolean dropOnFileFormatError) {
        StorageOpts storageOpts = new StorageOpts(enableStorage, dropOnFileFormatError, true);
        NamespaceDef namespaceDef = new NamespaceDef(storageOpts, namespace);

        String json = toJson(namespaceDef);
        rpcCallNoResults(OperationType.WRITE, OPEN_NAMESPACE, json);
    }

    private String toJson(Object object) {
        Gson gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .create();
        return gson.toJson(object);
    }

    @Override
    public void addIndex(String namespace, IndexDef index) {
        rpcCallNoResults(OperationType.WRITE, ADD_INDEX, namespace, toJson(index));
    }

    @Override
    public void modifyItem(int nsHash, String namespace, int format, byte[] data, int mode, String[] percepts,
                           int stateToken) {
        byte[] packedPercepts = new byte[0];
        if (percepts.length > 0) {
            throw new UnimplementedException();
        }
        rpcCallNoResults(OperationType.WRITE, MODIFY_ITEM, namespace, format, data, mode,
                packedPercepts, stateToken, 0);
    }

    @Override
    public void dropNamespace(String namespace) {
        rpcCallNoResults(OperationType.WRITE, DROP_NAMESPACE, namespace);
    }

    @Override
    public void closeNamespace(String namespace) {
        rpcCallNoResults(OperationType.WRITE, CLOSE_NAMESPACE, namespace);
    }

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

        RpcResponse rpcResponse = rpcCall(OperationType.READ, SELECT, queryData, flags,
                fetchCount, new byte[]{0});


        byte[] queryResultData = new byte[0];
        int requestId = -1;
        Object[] responseArguments = rpcResponse.getArguments();
        if (responseArguments.length > 0) {
            queryResultData = (byte[]) responseArguments[0];
        }
        if (responseArguments.length > 1) {
            requestId = (int) responseArguments[1];
        }

        return new QueryResult(requestId, queryResultData);
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
