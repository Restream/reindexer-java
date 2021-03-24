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
package ru.rt.restream.reindexer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.binding.TransactionContext;
import ru.rt.restream.reindexer.binding.cproto.ItemSerializer;
import ru.rt.restream.reindexer.binding.cproto.cjson.PayloadType;
import ru.rt.restream.reindexer.exceptions.ReindexerExceptionFactory;
import ru.rt.restream.reindexer.exceptions.StateInvalidatedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * An object that represents the context of a transaction.
 */
public class Transaction<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Transaction.class);

    /**
     * Namespace in which the transaction is executed.
     */
    private final ReindexerNamespace<T> namespace;

    /**
     * Binding to Reindexer instance.
     */
    private final Reindexer reindexer;

    /**
     * The futures list.
     */
    private final List<CompletableFuture<?>> futures = new ArrayList<>();

    /**
     * Indicates that the current transaction is started.
     */
    private boolean started;

    /**
     * Indicates that the current transaction is finalized.
     */
    private boolean finalized;

    /**
     * The transaction context.
     */
    private TransactionContext transactionContext;

    /**
     * Creates an instance.
     *
     * @param namespace the namespace
     * @param reindexer   a binding to Reindexer instance
     */
    public Transaction(ReindexerNamespace<T> namespace, Reindexer reindexer) {
        this.namespace = namespace;
        this.reindexer = reindexer;
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
        transactionContext = reindexer.getBinding().beginTx(namespace.getName());
        started = true;
        LOGGER.debug("rx: transaction started");
    }

    /**
     * Commits the current transaction.
     * Waits for worker threads to finish processing async requests.
     *
     * @throws IllegalStateException                    if the current transaction is finalized
     * @throws java.util.concurrent.CompletionException if there is an error while processing async requests
     */
    public void commit() {
        checkFinalized();
        if (!started) {
            return;
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        transactionContext.commit();
        finalized = true;
        LOGGER.debug("rx: transaction finalized with commit");
    }

    /**
     * Rollbacks the current transaction.
     * Waits for worker threads to finish processing async requests.
     * It is safe to call rollback after commit.
     */
    public void rollback() {
        if (!started || finalized) {
            return;
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).exceptionally(e -> null).join();
        transactionContext.rollback();
        finalized = true;
        LOGGER.debug("rx: transaction finalized with rollback");
    }

    private void checkFinalized() {
        if (finalized) {
            throw new IllegalStateException("Transaction is finalized");
        }
    }

    /**
     * Inserts the given item data in the current transaction.
     * Starts a transaction if not started.
     *
     * @param item the item data
     * @throws IllegalStateException if the current transaction is finalized
     */
    public void insert(T item) {
        start();
        modifyItem(item, Reindexer.MODE_INSERT, Consts.FORMAT_C_JSON);
    }

    /**
     * Inserts the given json-formatted item data in the current transaction.
     * Starts a transaction if not started.
     *
     * @param json the json-formatted item data
     * @throws IllegalStateException if the current transaction is finalized
     */
    public void insert(String json) {
        start();
        modifyItem(json, Reindexer.MODE_INSERT, Consts.FORMAT_JSON);
    }

    /**
     * Updates the given item data in the current transaction.
     * Starts a transaction if not started.
     *
     * @param item the item data
     * @throws IllegalStateException if the current transaction is finalized
     */
    public void update(T item) {
        start();
        modifyItem(item, Reindexer.MODE_UPDATE, Consts.FORMAT_C_JSON);
    }

    /**
     * Updates the given json-formatted item data in the current transaction.
     * Starts a transaction if not started.
     *
     * @param json the json-formatted item data
     * @throws IllegalStateException if the current transaction is finalized
     */
    public void update(String json) {
        start();
        modifyItem(json, Reindexer.MODE_UPDATE, Consts.FORMAT_JSON);
    }

    /**
     * Inserts or updates the given item data in the current transaction.
     * Starts a transaction if not started.
     *
     * @param item the item data
     * @throws IllegalStateException if the current transaction is finalized
     */
    public void upsert(T item) {
        start();
        modifyItem(item, Reindexer.MODE_UPSERT, Consts.FORMAT_C_JSON);
    }

    /**
     * Inserts or updates the given json-formatted item data in the current transaction.
     * Starts a transaction if not started.
     *
     * @param json the json-formatted item data
     * @throws IllegalStateException if the current transaction is finalized
     */
    public void upsert(String json) {
        start();
        modifyItem(json, Reindexer.MODE_UPSERT, Consts.FORMAT_JSON);
    }

    /**
     * Deletes the given item data in the current transaction.
     * Starts a transaction if not started.
     *
     * @param item the item data
     * @throws IllegalStateException if the current transaction is finalized
     */
    public void delete(T item) {
        start();
        modifyItem(item, Reindexer.MODE_DELETE, Consts.FORMAT_C_JSON);
    }

    /**
     * Deletes the given json-formatted item data in the current transaction.
     * Starts a transaction if not started.
     *
     * @param json the json-formatted item data
     * @throws IllegalStateException if the current transaction is finalized
     */
    public void delete(String json) {
        start();
        modifyItem(json, Reindexer.MODE_DELETE, Consts.FORMAT_JSON);
    }

    /**
     * Inserts the given item data in the current transaction asynchronously.
     * Starts a transaction if not started.
     *
     * @param item the item data
     * @return the {@link CompletableFuture}
     * @throws IllegalStateException if the current transaction is finalized
     */
    public CompletableFuture<T> insertAsync(T item) {
        start();
        return modifyItemAsync(item, Reindexer.MODE_INSERT, Consts.FORMAT_C_JSON);
    }

    /**
     * Inserts the given json-formatted item data in the current transaction asynchronously.
     * Starts a transaction if not started.
     *
     * @param json the json-formatted item data
     * @return the {@link CompletableFuture}
     * @throws IllegalStateException if the current transaction is finalized
     */
    public CompletableFuture<String> insertAsync(String json) {
        start();
        return modifyItemAsync(json, Reindexer.MODE_INSERT, Consts.FORMAT_JSON);
    }

    /**
     * Updates the given item data in the current transaction asynchronously.
     * Starts a transaction if not started.
     *
     * @param item the item data
     * @return the {@link CompletableFuture}
     * @throws IllegalStateException if the current transaction is finalized
     */
    public CompletableFuture<T> updateAsync(T item) {
        start();
        return modifyItemAsync(item, Reindexer.MODE_UPDATE, Consts.FORMAT_C_JSON);
    }

    /**
     * Updates the given json-formatted item data in the current transaction asynchronously.
     * Starts a transaction if not started.
     *
     * @param json the json-formatted item data
     * @return the {@link CompletableFuture}
     * @throws IllegalStateException if the current transaction is finalized
     */
    public CompletableFuture<String> updateAsync(String json) {
        start();
        return modifyItemAsync(json, Reindexer.MODE_UPDATE, Consts.FORMAT_JSON);
    }

    /**
     * Inserts or updates the given item data in the current transaction asynchronously.
     * Starts a transaction if not started.
     *
     * @param item the item data
     * @return the {@link CompletableFuture}
     * @throws IllegalStateException if the current transaction is finalized
     */
    public CompletableFuture<T> upsertAsync(T item) {
        start();
        return modifyItemAsync(item, Reindexer.MODE_UPSERT, Consts.FORMAT_C_JSON);
    }

    /**
     * Inserts or updates the given json-formatted item data in the current transaction asynchronously.
     * Starts a transaction if not started.
     *
     * @param json the json-formatted item data
     * @return the {@link CompletableFuture}
     * @throws IllegalStateException if the current transaction is finalized
     */
    public CompletableFuture<String> upsertAsync(String json) {
        start();
        return modifyItemAsync(json, Reindexer.MODE_UPSERT, Consts.FORMAT_JSON);
    }

    /**
     * Deletes the given item data in the current transaction asynchronously.
     * Starts a transaction if not started.
     *
     * @param item the item data
     * @return the {@link CompletableFuture}
     * @throws IllegalStateException if the current transaction is finalized
     */
    public CompletableFuture<T> deleteAsync(T item) {
        start();
        return modifyItemAsync(item, Reindexer.MODE_DELETE, Consts.FORMAT_C_JSON);
    }

    /**
     * Deletes the given json-formatted item data in the current transaction asynchronously.
     * Starts a transaction if not started.
     *
     * @param json the json-formatted item data
     * @return the {@link CompletableFuture}
     * @throws IllegalStateException if the current transaction is finalized
     */
    public CompletableFuture<String> deleteAsync(String json) {
        start();
        return modifyItemAsync(json, Reindexer.MODE_DELETE, Consts.FORMAT_JSON);
    }

    private <E> CompletableFuture<E> modifyItemAsync(E item, int mode, int format) {
        CompletableFuture<E> future = modifyItemAsyncInternal(item, mode, format, 1);
        futures.add(future);
        return future;
    }

    private <E> CompletableFuture<E> modifyItemAsyncInternal(E item, int mode, int format, int retryCount) {
        LOGGER.debug("rx: transaction modifyItemAsync, params=[{}, {}], retryCount={}", item, mode, retryCount);
        String[] precepts = namespace.getPrecepts();
        PayloadType payloadType = namespace.getPayloadType();
        int stateToken = payloadType == null ? 0 : payloadType.getStateToken();
        ItemSerializer<E> serializer = ItemSerializer.getInstance(item.getClass(), payloadType);
        byte[] data = serializer.serialize(item);
        return transactionContext.modifyItemAsync(data, format, mode, precepts, stateToken)
                .thenApplyAsync(rpcResponse -> {
                    if (rpcResponse.hasError()) {
                        throw ReindexerExceptionFactory.fromResponse(rpcResponse);
                    }
                    return item;
                })
                .thenApply(CompletableFuture::completedFuture)
                .exceptionally(error -> {
                    if (error.getCause() instanceof StateInvalidatedException && retryCount > 0) {
                        updatePayloadType();
                        return modifyItemAsyncInternal(item, mode, format,retryCount - 1);
                    }
                    return failedFuture(error);
                })
                .thenCompose(Function.identity());
    }

    private <E> CompletableFuture<E> failedFuture(Throwable t) {
        CompletableFuture<E> future = new CompletableFuture<>();
        future.completeExceptionally(t);
        return future;
    }

    private <E> void modifyItem(E item, int mode, int format) {
        LOGGER.debug("rx: transaction modifyItem, params=[{}, {}]", item, mode);
        String[] precepts = namespace.getPrecepts();
        for (int i = 0; i < 2; i++) {
            try {
                PayloadType payloadType = namespace.getPayloadType();
                int stateToken = payloadType == null ? 0 : payloadType.getStateToken();
                ItemSerializer<E> serializer = ItemSerializer.getInstance(item.getClass(), payloadType);
                byte[] data = serializer.serialize(item);
                transactionContext.modifyItem(data, format, mode, precepts, stateToken);
                break;
            } catch (StateInvalidatedException e) {
                LOGGER.debug("rx: transaction modifyItem state invalidated, update payload type");
                updatePayloadType();
            }
        }
    }

    private void updatePayloadType() {
        try {
            query().limit(0).execute().close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a {@link Query} with the current transaction for Update or Delete or Read.
     * Read-committed isolation is available for read operations.
     * Changes made in the current transaction is invisible to the current and another transactions.
     *
     * @return a {@link Query} with the current transaction
     */
    public Query<T> query() {
        return new Query<>(reindexer, namespace, transactionContext);
    }

}
