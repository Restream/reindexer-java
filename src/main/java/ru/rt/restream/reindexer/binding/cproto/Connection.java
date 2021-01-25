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

import ru.rt.restream.reindexer.ReindexerResponse;

import java.util.concurrent.CompletableFuture;

/**
 * A connection with a specific reindexer instance. Uses reindexer rpc protocol.
 * Commands are executed and results are returned within the context of a connection.
 */
public interface Connection extends AutoCloseable {

    /**
     * Call a rpc command with specified arguments.
     *
     * @param command command to invoke
     * @param args    command arguments
     * @return rpc call result
     */
    ReindexerResponse rpcCall(int command, Object... args);

    /**
     * Call a rpc command with specified arguments asynchronously.
     *
     * @param command command to invoke
     * @param args    command arguments
     * @return the {@link CompletableFuture}
     */
    CompletableFuture<ReindexerResponse> rpcCallAsync(int command, Object... args);

    /**
     * Returns true if the connection has an error.
     *
     * @return true if the connection has an error
     */
    boolean hasError();

    /**
     * Closes the connection.
     */
    @Override
    void close();

}
