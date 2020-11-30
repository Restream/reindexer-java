package ru.rt.restream.reindexer.binding.cproto;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.rt.restream.reindexer.binding.Binding;

import java.net.URI;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A simple standalone connection pool.
 * It is based on a blocking queue.
 *
 * @see ArrayBlockingQueue
 */
public class ConnectionPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionPool.class);

    /**
     * Available connections.
     */
    private final BlockingQueue<Connection> connections;

    /**
     * Connection timeout.
     */
    private final long connectionTimeout;

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
     * @param connectionTimeout  the connection timeout
     */
    public ConnectionPool(String url, int connectionPoolSize, long connectionTimeout) {
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
        connections = new ArrayBlockingQueue<>(connectionPoolSize);
        for (int i = 0; i < connectionPoolSize; i++) {
            Connection connection = new PhysicalConnection(uri.getHost(), uri.getPort());
            login(connection);
            connections.add(connection);
        }
        this.connectionTimeout = connectionTimeout;
    }

    private void login(Connection connection) {
        connection.rpcCall(Binding.LOGIN, user, password, database);
    }

    /**
     * Retrieves a {@link Connection} from the connection pool. If <code>connectionPoolSize</code> connections are
     * already in use, waits until a connection becomes available or <code>connectionTimeout</code> seconds elapsed.
     * When the application is finished using the connection, it must close it in order to return it to the pool.
     *
     * @return a {@link Connection} from the connection pool
     * @throws IllegalStateException if the connection pool is closed or a timeout occurred
     */
    public Connection getConnection() {
        if (closed.get()) {
            throw new IllegalStateException("Connection pool is closed");
        }
        Connection connection = null;
        try {
            connection = connections.poll(connectionTimeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
        if (connection == null) {
            throw new IllegalStateException("No available connections in pool");
        }
        return new PooledConnection(connection);
    }

    /**
     * Closes all unused pooled connections.
     * Exceptions while closing are written to the log.
     */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            for (Connection connection : connections) {
                try {
                    connection.close();
                } catch (Exception e) {
                    LOGGER.error("rx: connection close error", e);
                }
            }
        }
    }

    @RequiredArgsConstructor
    private class PooledConnection implements Connection {

        private final Connection connection;

        private boolean closed;

        @Override
        public RpcResponse rpcCall(int command, Object... args) {
            if (closed) {
                throw new IllegalStateException("Connection is closed");
            }
            return connection.rpcCall(command, args);
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            connections.add(connection);
            closed = true;
        }

    }

}
