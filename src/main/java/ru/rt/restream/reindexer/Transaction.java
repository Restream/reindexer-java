package ru.rt.restream.reindexer;

import ru.rt.restream.reindexer.binding.Binding;
import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.binding.cproto.ItemWriter;
import ru.rt.restream.reindexer.binding.cproto.json.JsonItemWriter;

/**
 * An object that represents the context of a transaction.
 */
public class Transaction<T> {

    /**
     * Namespace in which the transaction is executed.
     */
    private final ReindexerNamespace<T> namespace;

    /**
     * Binding to Reindexer instance.
     */
    private final Binding binding;

    /**
     * Current transaction id.
     */
    private long transactionId;

    /**
     * Indicates that the current transaction is started.
     */
    private boolean started;

    /**
     * Indicates that the current transaction is finalized.
     */
    private boolean finalized;

    /**
     * Creates an instance.
     *
     * @param namespace the namespace
     * @param binding   a binding to Reindexer instance
     */
    public Transaction(ReindexerNamespace<T> namespace, Binding binding) {
        this.namespace = namespace;
        this.binding = binding;
    }

    /**
     * Returns the current transaction id.
     *
     * @return the current transaction id
     */
    public long getTransactionId() {
        return transactionId;
    }

    /**
     * Starts a transaction.
     *
     * @throws IllegalStateException if the current transaction is finalized
     */
    public void start() {
        checkFinalized();
        if (started) {
            return;
        }
        transactionId = binding.beginTx(namespace.getName());
        started = true;
    }

    /**
     * Commits the current transaction.
     *
     * @throws IllegalStateException if the current transaction is finalized
     */
    public void commit() {
        commitWithCount();
    }

    /**
     * Commits the current transaction and returns the changes count.
     *
     * @return the changes count
     * @throws IllegalStateException if the current transaction is finalized
     */
    public long commitWithCount() {
        checkFinalized();
        if (!started) {
            return 0L;
        }
        long count = binding.commitTx(transactionId);
        finalized = true;
        return count;
    }

    /**
     * Rollbacks the current transaction.
     *
     * @throws IllegalStateException if the current transaction is finalized
     */
    public void rollback() {
        checkFinalized();
        if (!started) {
            return;
        }
        binding.rollback(transactionId);
        finalized = true;
    }

    private void checkFinalized() {
        if (finalized) {
            throw new IllegalStateException("Transaction is finalized");
        }
    }

    /**
     * Inserts or updates the given item data.
     * Starts a transaction if not started.
     *
     * @param item the item data
     * @throws IllegalStateException if the current transaction is finalized
     */
    public void upsert(T item) {
        start();

        int format = Consts.FORMAT_JSON;
        String[] percepts = new String[0];

        ByteBuffer buffer = new ByteBuffer();
        buffer.putVarInt64(format);
        ItemWriter<T> itemWriter = new JsonItemWriter<>();
        itemWriter.writeItem(buffer, item);

        binding.modifyItemTx(format, buffer.bytes(), Reindexer.MODE_UPSERT, percepts, 0, transactionId);
    }

    /**
     * Creates a {@link Query} with the current transaction for Update or Delete or Read.
     * Read-committed isolation is available for read operations.
     * Changes made in the current transaction is invisible to the current and another transactions.
     *
     * @return a {@link Query} with the current transaction
     */
    public Query<T> query() {
        return new Query<>(binding, namespace, this);
    }

}
