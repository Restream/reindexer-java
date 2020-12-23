package ru.rt.restream.reindexer.binding.cproto.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.rt.restream.reindexer.binding.cproto.Connection;
import ru.rt.restream.reindexer.binding.cproto.RpcResponse;
import ru.rt.restream.reindexer.exceptions.ReindexerException;

/**
 * Utility class for using a {@link Connection}.
 */
public final class ConnectionUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionUtils.class);

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
     * @return the {@link RpcResponse}
     * @throws ReindexerException in case of Reindexer error
     */
    public static RpcResponse rpcCall(Connection connection, int command, Object... args) {
        RpcResponse rpcResponse = connection.rpcCall(command, args);
        if (rpcResponse.hasError()) {
            throw new ReindexerException(rpcResponse.getErrorMessage());
        }
        return rpcResponse;
    }

    /**
     * Closes the given connection.
     * Exceptions while closing are written to the log.
     *
     * @param connection the connection to close
     */
    public static void close(Connection connection) {
        try {
            connection.close();
        } catch (Exception e) {
            LOGGER.error("rx: connection close error", e);
        }
    }

}
