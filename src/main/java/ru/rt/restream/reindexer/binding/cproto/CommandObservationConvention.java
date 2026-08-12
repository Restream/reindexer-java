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

import io.micrometer.common.KeyValues;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;

/**
 * An {@link ObservationConvention} to handle {@link CommandObservationContext} observations.
 */
final class CommandObservationConvention implements ObservationConvention<CommandObservationContext> {

    private static final String OBSERVATION_NAME = "reindexer.rpc";

    @Override
    public String getName() {
        return OBSERVATION_NAME;
    }

    @Override
    public String getContextualName(CommandObservationContext context) {
        return OBSERVATION_NAME + "." + context.getCommandName();
    }

    @Override
    public KeyValues getLowCardinalityKeyValues(CommandObservationContext context) {
        String commandName = context.getCommandName();
        String collectionName = context.getCollectionName();
        String responseStatusCode = context.getResponse() != null ? String.valueOf(context.getResponse().getCode()) : "";
        String executionType = context.getExecutionType().name();
        String networkTransport = "";
        String namespace = "";
        String serverAddress = "";
        String serverPort = "";
        CommandObservationContext.RemoteServerAddress remoteServerAddress = context.getRemoteServerAddress();
        if (remoteServerAddress != null) {
            networkTransport = remoteServerAddress.getProtocol();
            namespace = remoteServerAddress.getDatabase();
            serverAddress = remoteServerAddress.getHost();
            serverPort = String.valueOf(remoteServerAddress.getPort());
        }
        return KeyValues.of(
                "db.system.name", "reindexer",
                "db.command.name", commandName,
                "db.namespace", namespace,
                "db.collection.name", collectionName,
                "network.transport", networkTransport,
                "server.address", serverAddress,
                "server.port", serverPort,
                "code.execution_type", executionType,
                "db.response.status_code", responseStatusCode
        );
    }

    @Override
    public KeyValues getHighCardinalityKeyValues(CommandObservationContext context) {
        String threadId = String.valueOf(Thread.currentThread().getId());
        String threadName = Thread.currentThread().getName();
        String transactionId = context.getTransactionId();
        String requestId = context.getRequestId();
        return KeyValues.of(
                "thread.id", threadId,
                "thread.name", threadName,
                "db.reindexer.tx_id", transactionId,
                "db.reindexer.rq_id", requestId
        );
    }

    @Override
    public boolean supportsContext(Observation.Context context) {
        return context instanceof CommandObservationContext;
    }

}
