package ru.rt.restream.reindexer.binding.cproto;

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
    RpcResponse rpcCall(int command, Object... args);

}
