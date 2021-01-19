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
package ru.rt.restream.reindexer.binding.cproto.util;

import ru.rt.restream.reindexer.binding.cproto.Connection;
import ru.rt.restream.reindexer.ReindexerResponse;
import ru.rt.restream.reindexer.exceptions.ReindexerException;
import ru.rt.restream.reindexer.exceptions.ReindexerExceptionFactory;

/**
 * Utility class for using a {@link Connection}.
 */
public final class ConnectionUtils {

    private ConnectionUtils() {
        // utils
    }

    /**
     * Performs RPC call with no results.
     *
     * @param connection the connection to use
     * @param command    the command to use
     * @param args       the command arguments
     * @throws ReindexerException in case of Reindexer error
     */
    public static void rpcCallNoResults(Connection connection, int command, Object... args) {
        rpcCall(connection, command, args);
    }

    /**
     * Performs RPC call.
     *
     * @param connection the connection to use
     * @param command    the command to use
     * @param args       the command arguments
     * @return the {@link ReindexerResponse}
     * @throws ReindexerException in case of Reindexer error
     */
    public static ReindexerResponse rpcCall(Connection connection, int command, Object... args) {
        ReindexerResponse rpcResponse = connection.rpcCall(command, args);
        if (rpcResponse.hasError()) {
            throw ReindexerExceptionFactory.fromResponse(rpcResponse);
        }
        return rpcResponse;
    }

}
