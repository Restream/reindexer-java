package ru.rt.restream.reindexer.connector.binding.cproto;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.SneakyThrows;
import ru.rt.restream.reindexer.connector.binding.Binding;
import ru.rt.restream.reindexer.connector.binding.Command;
import ru.rt.restream.reindexer.connector.binding.def.IndexDef;
import ru.rt.restream.reindexer.connector.binding.def.NamespaceDef;
import ru.rt.restream.reindexer.connector.exceptions.ReindexerException;
import ru.rt.restream.reindexer.connector.exceptions.UnimplementedException;
import ru.rt.restream.reindexer.connector.StorageOpts;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

public class Cproto implements Binding {

    static final long CPROTO_MAGIC = 0xEEDD1132L;

    static final int CPROTO_VERSION = 0x101;

    static final int CPROTO_HDR_LEN = 16;

    static final int CPROTO_VERSION_COMPRESSION_FLAG = 1 << 10;

    static final int CPROTO_VERSION_MASK = 0x3FF;

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
        connection.rpcCall(Command.LOGIN, user, password, database);
    }

    @Override
    public void openNamespace(String namespace, boolean enableStorage, boolean dropOnFileFormatError) {
        StorageOpts storageOpts = new StorageOpts(enableStorage, dropOnFileFormatError, true);
        NamespaceDef namespaceDef = new NamespaceDef(storageOpts, namespace);

        String json = toJson(namespaceDef);
        RpcResult.Error error = rpcCallNoResults(OperationType.WRITE, Command.OPEN_NAMESPACE,
                ByteBuffer.wrap(json.getBytes()));

        if (!error.isOk()) {
            throw new ReindexerException(error.getMessage());
        }
    }

    private String toJson(Object object) {
        Gson gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .create();
        return gson.toJson(object);
    }

    @SneakyThrows
    @Override
    public void addIndex(String namespace, IndexDef index) {
        RpcResult.Error error = rpcCallNoResults(OperationType.WRITE, Command.ADD_INDEX, namespace, toJson(index));

        if (!error.isOk()) {
            throw new ReindexerException(error.getMessage());
        }
    }

    @Override
    public void modifyItem(int nsHash, String namespace, int format,
                                byte[] data, int mode, String[] percepts, int stateToken) {
        ByteBuffer packedPercepts = ByteBuffer.allocate(0);
        if (percepts.length > 0) {
            //TODO
            throw new UnimplementedException();
        }
        RpcResult.Error error = rpcCallNoResults(OperationType.WRITE, Command.MODIFY_ITEM, namespace, format,
                ByteBuffer.wrap(data), mode, packedPercepts, stateToken, 0);

        if (!error.isOk()) {
            throw new RuntimeException(error.getMessage());
        }
    }

    @Override
    public void dropNamespace(String namespace) {
        rpcCallNoResults(OperationType.WRITE, Command.DROP_NAMESPACE, namespace);
    }

    @Override
    public void closeNamespace(String namespace) {
        rpcCallNoResults(OperationType.WRITE, Command.CLOSE_NAMESPACE, namespace);
    }

    private RpcResult.Error rpcCallNoResults(OperationType operationType, int command, Object... args) {
        RpcResult result = rpcCall(operationType, command, args);
        return result.getError();
    }


    private RpcResult rpcCall(OperationType operationType, int command, Object... args) {
        return connection.rpcCall(command, args);
    }

}
