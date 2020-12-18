package ru.rt.restream.reindexer.binding.cproto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.rt.restream.reindexer.binding.TransactionContext;
import ru.rt.restream.reindexer.exceptions.ReindexerException;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A transaction context which establish a connection to the Reindexer instance via RPC.
 */
public class CprotoTransactionContext implements TransactionContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(CprotoTransactionContext.class);

    private static final int ADD_TX_ITEM = 26;

    private static final int UPDATE_QUERY_TX = 31;

    private static final int DELETE_QUERY_TX = 30;

    private static final int COMMIT_TX = 27;

    private static final int ROLLBACK_TX = 28;

    private final Lock lock = new ReentrantLock();

    private final long transactionId;

    private final Connection connection;

    /**
     * Creates an instance.
     *
     * @param transactionId the transaction id
     * @param connection    the connection in which the transaction is started
     */
    public CprotoTransactionContext(long transactionId, Connection connection) {
        this.transactionId = transactionId;
        this.connection = connection;
    }

    @Override
    public void modifyItem(int format, byte[] data, int mode, String[] precepts, int stateToken) {
        byte[] packedPrecepts = new byte[0];
        if (precepts.length > 0) {
            ByteBuffer buffer = new ByteBuffer();
            buffer.putVarUInt32(precepts.length);
            for (String precept : precepts) {
                buffer.putVString(precept);
            }
            packedPrecepts = buffer.bytes();
        }
        rpcCall(ADD_TX_ITEM, format, data, mode, packedPrecepts, stateToken, transactionId);
    }

    @Override
    public void updateQuery(byte[] queryData) {
        rpcCall(UPDATE_QUERY_TX, queryData, transactionId);
    }

    @Override
    public void deleteQuery(byte[] queryData) {
        rpcCall(DELETE_QUERY_TX, queryData, transactionId);
    }

    @Override
    public void commit() {
        rpcCall(COMMIT_TX, transactionId);
    }

    @Override
    public void rollback() {
        rpcCall(ROLLBACK_TX, transactionId);
    }

    private void rpcCall(int command, Object... args) {
        lock.lock();
        try {
            RpcResponse rpcResponse = connection.rpcCall(command, args);
            if (rpcResponse.hasError()) {
                throw new ReindexerException(rpcResponse.getErrorMessage());
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Closes the connection.
     */
    @Override
    public void close() {
        lock.lock();
        try {
            connection.close();
        } catch (Exception e) {
            LOGGER.error("rx: connection close error");
        } finally {
            lock.unlock();
        }
    }

}
