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

import ru.rt.restream.reindexer.binding.Binding;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple standalone connection pool.
 */
public class ConnectionPool {

    /**
     * Scheduler for async I/O processing.
     */
    private final ScheduledExecutorService scheduler;

    /**
     * Available connections.
     */
    private final List<Connection> connections;

    /**
     * Generator of identifiers for obtaining a connection.
     */
    private final AtomicInteger next = new AtomicInteger(0);

    /**
     * Indicates that if this pool is closed.
     */
    private final AtomicBoolean closed = new AtomicBoolean(false);

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
     * Construct the connection pool instance to the given database URL.
     *
     * @param url                a database url of the form cproto://host:port/database_name
     * @param connectionPoolSize the connection pool size
     * @param requestTimeout     the request timeout
     */
    public ConnectionPool(String url, int connectionPoolSize, long requestTimeout) {
        URI uri = URI.create(url);
        database = uri.getPath().substring(1);
        String userInfo = uri.getUserInfo();
        if (userInfo != null) {
            String[] userInfoArray = userInfo.split(":");
            if (userInfoArray.length == 2) {
                user = userInfoArray[0];
                password = userInfoArray[1];
            } else {
                throw new IllegalArgumentException();
            }
        } else {
            user = "";
            password = "";
        }
        scheduler = Executors.newScheduledThreadPool(connectionPoolSize * 2);
        List<Connection> connections = new ArrayList<>(connectionPoolSize);
        for (int i = 0; i < connectionPoolSize; i++) {
            Connection connection = new PhysicalConnection(uri.getHost(), uri.getPort(), requestTimeout, scheduler);
            login(connection);
            connections.add(connection);
        }
        this.connections = Collections.unmodifiableList(connections);
    }

    private void login(Connection connection) {
        connection.rpcCall(Binding.LOGIN, user, password, database);
    }

    /**
     * Returns the next {@link Connection} from the connection pool.
     *
     * @return a {@link Connection} from the connection pool
     * @throws IllegalStateException if the connection pool is closed
     */
    public Connection getConnection() {
        if (closed.get()) {
            throw new IllegalStateException("Connection pool is closed");
        }
        return connections.get(next.getAndIncrement() % connections.size());
    }

    /**
     * Closes all unused pooled connections.
     * Exceptions while closing are written to the log.
     */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            connections.forEach(Connection::close);
            scheduler.shutdown();
        }
    }

}
