/**
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.rt.restream.reindexer.binding.cproto.util.ConnectionUtils;

import java.util.function.Function;

/**
 * An executor that executes each submitted function using one of the pooled connections.
 */
public class ConnectionPoolExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionPoolExecutor.class);

    private final ConnectionPool pool;

    /**
     * Creates an instance.
     *
     * @param pool the connection pool to use
     */
    public ConnectionPoolExecutor(ConnectionPool pool) {
        this.pool = pool;
    }

    /**
     * Executes the given function using one of the pooled connections.
     * If <code>keepAlive</code> is true, then the connection is not closed after executing the given function,
     * this is useful if the connection is used for transactional requests.
     * The connection is force closed in case of an error while executing the given function.
     *
     * @param <T>       return type
     * @param function  a {@link Function} that accepts a {@link Connection} and returns an object of type {@link T}
     * @param keepAlive if true, then the connection is not closed after executing the given function
     * @return an object of type {@link T}
     */
    public <T> T executeInConnection(Function<Connection, T> function, boolean keepAlive) {
        Connection connection = pool.getConnection();
        try {
            return function.apply(connection);
        } catch (Exception e) {
            LOGGER.error("rx: executeInConnection error - force closing a connection", e);
            ConnectionUtils.close(connection);
            throw e;
        } finally {
            if (!keepAlive) {
                ConnectionUtils.close(connection);
            }
        }
    }

    /**
     * Closes the connection pool.
     */
    public void close() {
        pool.close();
    }

}
