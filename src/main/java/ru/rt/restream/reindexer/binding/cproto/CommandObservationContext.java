/*
 * Copyright 2020-present Restream
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
import com.google.gson.JsonSyntaxException;
import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.RequestReplySenderContext;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;
import ru.rt.restream.reindexer.ReindexerResponse;
import ru.rt.restream.reindexer.binding.Binding;
import ru.rt.restream.reindexer.binding.definition.NamespaceDefinition;

import java.net.URI;

/**
 * A context for command observation.
 */
final class CommandObservationContext extends RequestReplySenderContext<Object, ReindexerResponse> {

    private static final Gson GSON = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();

    private final int command;

    private final Object[] arguments;

    private String collectionName;

    private RemoteServerAddress remoteServerAddress;

    CommandObservationContext(int command, Object... arguments) {
        super((carrier, key, value) -> {}, Kind.CLIENT);
        this.command = command;
        this.arguments = arguments;
    }

    String getCommandName() {
        switch (command) {
            case Binding.OPEN_NAMESPACE:
                return "openNamespace";
            case Binding.CLOSE_NAMESPACE:
                return "closeNamespace";
            case Binding.DROP_NAMESPACE:
                return "dropNamespace";
            case Binding.ADD_INDEX:
                return "addIndex";
            case Binding.UPDATE_INDEX:
                return "updateIndex";
            case Binding.DROP_INDEX:
                return "dropIndex";
            case Binding.MODIFY_ITEM:
                return "modifyItem";
            case Binding.SELECT:
                return "selectQuery";
            case Binding.UPDATE_QUERY:
                return "updateQuery";
            case Binding.UPDATE_QUERY_TX:
                return "updateQueryTx";
            case Binding.DELETE_QUERY:
                return "deleteQuery";
            case Binding.DELETE_QUERY_TX:
                return "deleteQueryTx";
            case Binding.SELECT_SQL:
                return "selectSql";
            case Binding.FETCH_RESULTS:
                return "fetchResults";
            case Binding.CLOSE_RESULTS:
                return "closeResults";
            case Binding.START_TRANSACTION:
                return "startTransaction";
            case Binding.ADD_TX_ITEM:
                return "addTxItem";
            case Binding.COMMIT_TX:
                return "commitTx";
            case Binding.ROLLBACK_TX:
                return "rollbackTx";
            case Binding.PING:
                return "ping";
            case Binding.GET_META:
                return "getMeta";
            case Binding.PUT_META:
                return "putMeta";
            default:
                // Fallback to command code.
                return String.valueOf(command);
        }
    }

    String getCollectionName() {
        if (collectionName == null) {
            collectionName = extractCollectionName();
        }
        return collectionName;
    }

    private String extractCollectionName() {
        switch (command) {
            case Binding.OPEN_NAMESPACE: {
                // For openNamespace command, the [0] argument is a JSON string representing the namespace definition.
                String json = ArrayUtils.get(arguments, 0, "").toString();
                try {
                    NamespaceDefinition namespace = GSON.fromJson(json, NamespaceDefinition.class);
                    return namespace.getName() != null ? namespace.getName() : "";
                } catch (JsonSyntaxException ignored) {
                    // Return an empty string if the JSON string is invalid.
                    return "";
                }
            }
            case Binding.UPDATE_QUERY:
            case Binding.UPDATE_QUERY_TX:
            case Binding.DELETE_QUERY:
            case Binding.DELETE_QUERY_TX:
            case Binding.SELECT: {
                // Command arguments[0] is a byte array of query data.
                Object queryData = ArrayUtils.get(arguments, 0, null);
                // Read the variable length string from the buffer which is a namespace name.
                return queryData instanceof byte[]
                        ? new ByteBuffer((byte[]) queryData).rewind().getVString() : "";
            }
            case Binding.DROP_NAMESPACE:
            case Binding.CLOSE_NAMESPACE:
            case Binding.ADD_INDEX:
            case Binding.UPDATE_INDEX:
            case Binding.DROP_INDEX:
            case Binding.MODIFY_ITEM:
            case Binding.PUT_META:
            case Binding.GET_META:
            case Binding.START_TRANSACTION:
                // Command arguments[0] is the namespace name.
                return ArrayUtils.get(arguments, 0, "").toString();
            default:
                return "";
        }
    }

    String getTransactionId() {
        switch (command) {
            case Binding.ADD_TX_ITEM:
                // Command arguments[5] is the transaction id.
                return ArrayUtils.get(arguments, 5, "").toString();
            case Binding.UPDATE_QUERY_TX:
            case Binding.DELETE_QUERY_TX:
                // Command arguments[1] is the transaction id.
                return ArrayUtils.get(arguments, 1, "").toString();
            case Binding.START_TRANSACTION:
                // Response arguments[0] is the transaction id.
                return getResponse() != null
                        ? ArrayUtils.get(getResponse().getArguments(), 0, "").toString() : "";
            case Binding.COMMIT_TX:
            case Binding.ROLLBACK_TX:
                // Command arguments[0] is the transaction id.
                return ArrayUtils.get(arguments, 0, "").toString();
            default:
                return "";
        }
    }

    String getRequestId() {
        switch (command) {
            case Binding.FETCH_RESULTS:
            case Binding.CLOSE_RESULTS:
                // Command arguments[0] is the request id.
                return ArrayUtils.get(arguments, 0, "").toString();
            case Binding.SELECT:
            case Binding.SELECT_SQL:
                // Response arguments[1] is the request id.
                return getResponse() != null
                        ? ArrayUtils.get(getResponse().getArguments(), 1, "").toString() : "";
            default:
                return "";
        }
    }

    ExecutionType getExecutionType() {
        return Thread.currentThread().getName().startsWith(ConnectionPool.ConnectionThreadFactory.POOL_NAME_PREFIX)
                ? ExecutionType.ASYNC : ExecutionType.SYNC;
    }

    RemoteServerAddress getRemoteServerAddress() {
        if (remoteServerAddress == null && getRemoteServiceAddress() != null) {
            URI uri = URI.create(getRemoteServiceAddress());
            String path = uri.getPath();
            String database = path != null && path.startsWith("/") ? path.substring(1) : path;
            remoteServerAddress = new RemoteServerAddress(
                    uri.getScheme() != null ? uri.getScheme() : "",
                    uri.getHost() != null ? uri.getHost() : "",
                    database != null ? database : "",
                    uri.getPort()
            );
        }
        return remoteServerAddress;
    }

    /**
     * Code execution type for a command being run.
     */
    enum ExecutionType {

        /**
         * Synchronous execution. Usually called from the user's thread.
         */
        SYNC,

        /**
         * Asynchronous execution. Always called from the {@link ConnectionPool.ConnectionThreadFactory#POOL_NAME_PREFIX} thread.
         */
        ASYNC
    }

    /**
     * Represents a remote server address i.e., protocol, host, database, and port.
     */
    @Getter
    @RequiredArgsConstructor
    static final class RemoteServerAddress {
        private final String protocol;
        private final String host;
        private final String database;
        private final int port;
    }

}
