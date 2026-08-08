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
import io.micrometer.common.KeyValues;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;
import org.apache.commons.lang3.ArrayUtils;
import ru.rt.restream.reindexer.binding.Binding;
import ru.rt.restream.reindexer.binding.definition.NamespaceDefinition;

import java.net.URI;

/**
 * An {@link ObservationConvention} to handle {@link CommandObservationContext} observations.
 */
final class CommandObservationConvention implements ObservationConvention<CommandObservationContext> {

    private static final String OBSERVATION_NAME = "reindexer.rpc";

    private final Gson gson = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();

    @Override
    public String getName() {
        return OBSERVATION_NAME;
    }

    @Override
    public String getContextualName(CommandObservationContext context) {
        return OBSERVATION_NAME + "." + getCommandName(context.getCommand());
    }

    @Override
    public KeyValues getLowCardinalityKeyValues(CommandObservationContext context) {
        String command = getCommandName(context.getCommand());
        String collection = getCollectionName(context);
        String responseStatusCode = context.getResponse() != null ? String.valueOf(context.getResponse().getCode()) : "";
        String networkTransport = "";
        String namespace = "";
        String serverAddress = "";
        String serverPort = "";
        if (context.getRemoteServiceAddress() != null) {
            URI uri = URI.create(context.getRemoteServiceAddress());
            networkTransport = uri.getScheme();
            namespace = uri.getPath().substring(1);
            serverAddress = uri.getHost();
            serverPort = String.valueOf(uri.getPort());
        }
        return KeyValues.of(
                "db.system.name", "reindexer",
                "db.command.name", command,
                "db.namespace", namespace,
                "db.collection.name", collection,
                "network.transport", networkTransport,
                "server.address", serverAddress,
                "server.port", serverPort,
                "db.response.status_code", responseStatusCode
        );
    }

    @Override
    public KeyValues getHighCardinalityKeyValues(CommandObservationContext context) {
        String transactionId = getTransactionId(context);
        String requestId = getRequestId(context);
        return KeyValues.of(
                "db.reindexer.tx_id", transactionId,
                "db.reindexer.rq_id", requestId
        );
    }

    @Override
    public boolean supportsContext(Observation.Context context) {
        return context instanceof CommandObservationContext;
    }

    private String getCommandName(int command) {
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

    private String getCollectionName(CommandObservationContext context) {
        switch (context.getCommand()) {
            case Binding.OPEN_NAMESPACE:
            case Binding.DROP_NAMESPACE:
            case Binding.CLOSE_NAMESPACE:
            case Binding.ADD_INDEX:
            case Binding.UPDATE_INDEX:
            case Binding.DROP_INDEX:
            case Binding.MODIFY_ITEM:
            case Binding.PUT_META:
            case Binding.GET_META:
            case Binding.START_TRANSACTION:
                // Command arguments[0] is the namespace.
                String value = ArrayUtils.get(context.getArguments(), 0, "").toString();
                if (context.getCommand() == Binding.OPEN_NAMESPACE) {
                    // For openNamespace command, the [0] argument is a JSON string representing the namespace definition.
                    try {
                        NamespaceDefinition namespace = gson.fromJson(value, NamespaceDefinition.class);
                        return namespace.getName() != null ? namespace.getName() : "";
                    } catch (JsonSyntaxException ignored) {
                        // Return an empty string if the JSON string is invalid.
                        return "";
                    }
                }
                return value;
            default:
                return "";
        }
    }

    private String getTransactionId(CommandObservationContext context) {
        switch (context.getCommand()) {
            case Binding.ADD_TX_ITEM:
                // Command arguments[5] is the transaction id.
                return ArrayUtils.get(context.getArguments(), 5, "").toString();
            case Binding.UPDATE_QUERY_TX:
            case Binding.DELETE_QUERY_TX:
                // Command arguments[1] is the transaction id.
                return ArrayUtils.get(context.getArguments(), 1, "").toString();
            case Binding.START_TRANSACTION:
                // Response arguments[0] is the transaction id.
                return context.getResponse() != null
                        ? ArrayUtils.get(context.getResponse().getArguments(), 0, "").toString()
                        : "";
            case Binding.COMMIT_TX:
            case Binding.ROLLBACK_TX:
                // Command arguments[0] is the transaction id.
                return ArrayUtils.get(context.getArguments(), 0, "").toString();
            default:
                return "";
        }
    }

    private String getRequestId(CommandObservationContext context) {
        switch (context.getCommand()) {
            case Binding.FETCH_RESULTS:
            case Binding.CLOSE_RESULTS:
                // Command arguments[0] is the request id.
                return ArrayUtils.get(context.getArguments(), 0, "").toString();
            case Binding.SELECT:
            case Binding.SELECT_SQL:
                // Response arguments[1] is the request id.
                return context.getResponse() != null
                        ? ArrayUtils.get(context.getResponse().getArguments(), 1, "").toString()
                        : "";
            default:
                return "";
        }
    }

}
