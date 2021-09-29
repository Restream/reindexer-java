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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.rt.restream.reindexer.binding.Binding;
import ru.rt.restream.reindexer.binding.cproto.util.ConnectionUtils;
import ru.rt.restream.reindexer.exceptions.NetworkException;
import ru.rt.restream.reindexer.exceptions.ReindexerException;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A simple standalone connection pool.
 */
public class ConnectionPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionPool.class);

    /**
     * Read/Write lock.
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * The {@link DataSourceFactory} for obtaining a {@link DataSource}.
     */
    private final DataSourceFactory dataSourceFactory;

    /**
     * Scheduler for async I/O processing.
     */
    private final ScheduledThreadPoolExecutor scheduler;

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
     * URIs to connect.
     */
    private final List<URI> uris;

    /**
     * Request timeout.
     */
    private final Duration timeout;

    /**
     * Current {@link DataSource}.
     */
    private DataSource dataSource;

    /**
     * Construct the connection pool instance to the given database URL.
     *
     * @param uris               a database urls of the form cproto://host:port/database_name
     * @param dataSourceFactory  the {@link DataSourceFactory} to use
     * @param connectionPoolSize the connection pool size
     * @param requestTimeout     the request timeout
     */
    public ConnectionPool(List<URI> uris, DataSourceFactory dataSourceFactory,
                          int connectionPoolSize, Duration requestTimeout) {
        this.uris = uris;
        this.dataSourceFactory = dataSourceFactory;
        scheduler = new ScheduledThreadPoolExecutor(connectionPoolSize * 2 + 1);
        scheduler.setRemoveOnCancelPolicy(true);
        connections = new ArrayList<>(connectionPoolSize);
        timeout = requestTimeout;
        dataSource = getDataSource(connectionPoolSize);
        scheduler.scheduleWithFixedDelay(new PingTask(), 0, 1, TimeUnit.MINUTES);
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
        int id = next.getAndUpdate(i -> ++i < connections.size() ? i : 0);
        Connection connection;
        lock.readLock().lock();
        try {
            connection = connections.get(id);
        } finally {
            lock.readLock().unlock();
        }
        if (connection.hasError()) {
            lock.writeLock().lock();
            try {
                connection = connections.get(id);
                if (connection.hasError()) {
                    try {
                        connection = dataSource.getConnection(timeout, scheduler);
                        connections.set(id, connection);
                    } catch (NetworkException e) {
                        LOGGER.error("rx: connection-{} to {} failed with error", id, dataSource, e);
                        dataSource = getDataSource(connections.size());
                        connection = connections.get(id);
                    }
                    LOGGER.debug("rx: connection-{} reconnected to {}", id, dataSource);
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
        return connection;
    }

    private DataSource getDataSource(int connectionPoolSize) {
        Instant connectionDeadline = Instant.now().plus(timeout);
        for (; ; ) {
            if (Instant.now().isAfter(connectionDeadline)) {
                throw new IllegalStateException("Connection timeout: no available data source to connect");
            }
            DataSource dataSource = dataSourceFactory.getDataSource(uris);
            if (dataSource == null) {
                throw new IllegalArgumentException("dataSource cannot be null");
            }
            LOGGER.debug("rx: trying to connect to {}", dataSource);
            try {
                for (int i = 0; i < connectionPoolSize; i++) {
                    Connection newConnection = dataSource.getConnection(timeout, scheduler);
                    if (i < connections.size()) {
                        Connection oldConnection = connections.get(i);
                        oldConnection.close();
                        connections.set(i, newConnection);
                    } else {
                        connections.add(newConnection);
                    }
                }
                return dataSource;
            } catch (NetworkException e) {
                LOGGER.error("rx: connection to {} failed with error", dataSource, e);
            }
            try {
                TimeUnit.SECONDS.sleep(1L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ReindexerException("Interrupted while waiting for data source to connect");
            }
        }
    }

    /**
     * Closes all unused pooled connections.
     * Exceptions while closing are written to the log.
     */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            lock.readLock().lock();
            try {
                connections.forEach(Connection::close);
            } finally {
                lock.readLock().unlock();
            }
            scheduler.shutdown();
        }
    }

    private class PingTask implements Runnable {

        @Override
        public void run() {
            for (int i = 0; i < connections.size(); i++) {
                Connection connection;
                lock.readLock().lock();
                try {
                    connection = connections.get(i);
                } finally {
                    lock.readLock().unlock();
                }
                if (connection.hasError()) {
                    continue;
                }
                try {
                    ConnectionUtils.rpcCallNoResults(connection, Binding.PING);
                    LOGGER.debug("rx: connection-{} ping ok", i);
                } catch (Exception e) {
                    LOGGER.error("rx: connection-{} ping error", i, e);
                }
            }
        }

    }

}
