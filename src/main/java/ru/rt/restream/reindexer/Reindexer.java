/*
 * Copyright 2020-present Restream
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
import ru.rt.restream.reindexer.annotations.ReindexAnnotationScanner;
import ru.rt.restream.reindexer.binding.Binding;
import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.binding.QueryResult;
import ru.rt.restream.reindexer.binding.RequestContext;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.binding.cproto.ItemSerializer;
import ru.rt.restream.reindexer.binding.cproto.cjson.CjsonItemReader;
import ru.rt.restream.reindexer.binding.cproto.cjson.CtagMatcher;
import ru.rt.restream.reindexer.binding.cproto.cjson.PayloadType;
import ru.rt.restream.reindexer.binding.definition.IndexDefinition;
import ru.rt.restream.reindexer.binding.definition.NamespaceDefinition;
import ru.rt.restream.reindexer.exceptions.IndexConflictException;
import ru.rt.restream.reindexer.exceptions.StateInvalidatedException;
import ru.rt.restream.reindexer.expression.PreceptExpression;
import ru.rt.restream.reindexer.util.NativeUtils;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class Reindexer implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Reindexer.class);

    static final int MODE_UPDATE = 0;

    static final int MODE_INSERT = 1;

    static final int MODE_UPSERT = 2;

    static final int MODE_DELETE = 3;

    private final Binding binding;

    private final ReindexScanner reindexScanner = new ReindexAnnotationScanner();

    protected final Map<String, ReindexerNamespace<?>> namespaceMap = new ConcurrentHashMap<>();

    protected Reindexer(Binding binding) {
        this.binding = binding;
    }

    @Override
    public void close() {
        binding.close();
    }

    /**
     * Opens or creates new namespace and indexes based on passed item class.
     *
     * @param <T>       the item type
     * @param name      the namespace name
     * @param options   namespace options
     * @param itemClass the item class
     *
     * @return object, that provides methods for manipulating namespace data
     */
    public <T> Namespace<T> openNamespace(String name, NamespaceOptions options, Class<T> itemClass) {
        ReindexerNamespace<?> namespace = namespaceMap.computeIfAbsent(name,
                k -> doOpenNamespace(name, options, itemClass));
        if (namespace.getItemClass() != itemClass) {
            String msg = String.format("Wrong namespace item type: namespace already opened with item class %s",
                    namespace.getName());
            throw new RuntimeException(msg);
        }
        return (Namespace<T>) namespace;
    }

    /**
     * Creates new namespace and indexes based on passed item class.
     *
     * @param <T>       the item type
     * @param name      the namespace name
     * @param options   namespace options
     * @param itemClass the item class
     *
     * @return object, that provides methods for manipulating namespace data
     */
    private <T> ReindexerNamespace<T> doOpenNamespace(String name, NamespaceOptions options, Class<T> itemClass) {

        ReindexerNamespace<T> namespace = ReindexerNamespace.<T>builder()
                .name(name)
                .itemClass(Objects.requireNonNull(itemClass))
                .enableStorage(options.isEnableStorage())
                .createStorageIfMissing(options.isCreateStorageIfMissing())
                .disableObjCache(options.isDisableObjCache())
                .dropOnIndexConflict(options.isDropOnIndexesConflict())
                .dropStorageOnFileFormatError(options.isDropOnFileFormatError())
                .objCacheItemsCount(options.getObjCacheItemsCount())
                .indexes(reindexScanner.parseIndexes(itemClass))
                .reindexer(this)
                .build();
        // Initialize the namespace instance.
        namespace.initialize();

        try {
            binding.openNamespace(NamespaceDefinition.fromNamespace(namespace));
            for (ReindexerIndex index : namespace.getIndexes()) {
                IndexDefinition indexDefinition = IndexDefinition.fromIndex(index);
                binding.addIndex(name, indexDefinition);
            }
        } catch (IndexConflictException e) {
            if (namespace.isDropOnIndexConflict()) {
                binding.dropNamespace(name);
            } else {
                binding.closeNamespace(name);
            }
        } catch (Exception e) {
            binding.closeNamespace(name);
            throw e;
        }

        return namespace;
    }

    /**
     * Drop whole namespace from database.
     *
     * @param namespaceName namespace name to drop
     */
    public void dropNamespace(String namespaceName) {
        namespaceMap.remove(namespaceName);
        binding.dropNamespace(namespaceName);
    }

    /**
     * Inserts the given item data.
     *
     * @param <T>           the item type
     * @param namespaceName the namespace name
     * @param item          the item data
     * @return the inserted {@link T} item
     */
    public <T> T insert(String namespaceName, T item) {
        return modifyItem(namespaceName, item, MODE_INSERT, Consts.FORMAT_C_JSON);
    }

    /**
     * Inserts the given json-formatted item data.
     * Returns the server-generated {@link T} item, which may contain fields not present
     * in the input JSON, or {@link Optional#empty()} if the server returns no data.
     *
     * @param namespaceName the namespace name
     * @param json          the json-formatted item data
     * @return the server-generated {@link T} item, or {@link Optional#empty()} if no data was returned
     */
    public <T> Optional<T> insert(String namespaceName, String json) {
        return Optional.ofNullable(modifyItem(namespaceName, json, MODE_INSERT, Consts.FORMAT_JSON));
    }

    /**
     * Updates the given item data.
     *
     * @param <T>           the item type
     * @param namespaceName the namespace name
     * @param item          the item data
     * @return the updated {@link T} item
     */
    public <T> T update(String namespaceName, T item) {
        return modifyItem(namespaceName, item, MODE_UPDATE, Consts.FORMAT_C_JSON);
    }

    /**
     * Updates the given json-formatted item data.
     * Returns the server-generated {@link T} item, which may contain fields not present
     * in the input JSON, or {@link Optional#empty()} if the server returns no data.
     *
     * @param namespaceName the namespace name
     * @param json          the json-formatted item data
     * @return the server-generated {@link T} item, or {@link Optional#empty()} if no data was returned
     */
    public <T> Optional<T> update(String namespaceName, String json) {
        return Optional.ofNullable(modifyItem(namespaceName, json, MODE_UPDATE, Consts.FORMAT_JSON));
    }

    /**
     * Inserts or updates the given item data.
     *
     * @param <T>           the item type
     * @param namespaceName the namespace name
     * @param item          the item data
     * @return the upserted {@link T} item
     */
    public <T> T upsert(String namespaceName, T item) {
        return modifyItem(namespaceName, item, MODE_UPSERT, Consts.FORMAT_C_JSON);
    }

    /**
     * Inserts or updates the given json-formatted item data.
     * Returns the server-generated {@link T} item, which may contain fields not present
     * in the input JSON, or {@link Optional#empty()} if the server returns no data.
     *
     * @param namespaceName the namespace name
     * @param json          the json-formatted item data
     * @return the server-generated {@link T} item, or {@link Optional#empty()} if no data was returned
     */
    public <T> Optional<T> upsert(String namespaceName, String json) {
        return Optional.ofNullable(modifyItem(namespaceName, json, MODE_UPSERT, Consts.FORMAT_JSON));
    }

    /**
     * Deletes the given item data.
     *
     * @param <T>           the item type
     * @param namespaceName the namespace name
     * @param item          the item data
     */
    public <T> void delete(String namespaceName, T item) {
        modifyItem(namespaceName, item, MODE_DELETE, Consts.FORMAT_C_JSON);
    }

    /**
     * Deletes the given json-formatted item data.
     *
     * @param namespaceName the namespace name
     * @param json          the json-formatted item data
     */
    public void delete(String namespaceName, String json) {
        modifyItem(namespaceName, json, MODE_DELETE, Consts.FORMAT_JSON);
    }

    /**
     * Begin a unit of work and return the associated namespace Transaction object.
     *
     * @param <T>           the item type
     * @param namespaceName the namespace name
     * @param itemClass     the item class
     * @return a Transaction instance
     */
    public <T> Transaction<T> beginTransaction(String namespaceName, Class<T> itemClass) {
        ReindexerNamespace<T> namespace = getNamespace(namespaceName, itemClass);
        Transaction<T> transaction = new Transaction<>(namespace, this);
        transaction.start();
        return transaction;
    }

    public<T> QueryResultIterator<T> execSql(String query, Class<T> itemClass) {
        LOGGER.debug(query);
        String[] words = query.split("\\s+");
        String namespaceName = null;
        for (int i = 0; i < words.length; i++) {
            String word = words[i];
            if ("FROM".equalsIgnoreCase(word)) {
                namespaceName = words[++i];
                break;
            }
        }
        if (namespaceName == null) {
            throw new RuntimeException("Invalid select query, namespace name not found");
        }
        ReindexerNamespace<T> namespace = getNamespace(namespaceName, itemClass);
        PayloadType pt = namespace.getPayloadType();
        long[] ptVersions = pt == null ? new long[]{0} : new long[]{pt.getVersion() ^ pt.getStateToken()};
        RequestContext ctx = binding.select(query, false, Integer.MAX_VALUE, ptVersions);
        updatePayloadType(ctx.getQueryResult(), namespace);
        return new QueryResultIterator<>(namespace, itemClass, ctx, null, Integer.MAX_VALUE);
    }

    public void updateSql(String query) {
        LOGGER.debug(query);
        binding.select(query, false, 0, new long[] {0L});
    }

    /**
     * Creates new Query for building request
     *
     * @param <T>           the item type
     * @param namespaceName the namespace name
     * @param clazz         the item class
     * @return builder for building request
     */
    public <T> Query<T> query(String namespaceName, Class<T> clazz) {
        ReindexerNamespace<T> namespace = getNamespace(namespaceName, clazz);
        return new Query<>(this, namespace, null);
    }

    public void addIndex(String namespaceName, ReindexerIndex index) {
        IndexDefinition indexDefinition = IndexDefinition.fromIndex(index);
        binding.addIndex(namespaceName, indexDefinition);
    }

    public void updateIndex(String namespaceName, ReindexerIndex index) {
        IndexDefinition indexDefinition = IndexDefinition.fromIndex(index);
        binding.updateIndex(namespaceName, indexDefinition);
    }

    public void dropIndex(String namespaceName, String indexName) {
        binding.dropIndex(namespaceName, indexName);
    }

    /**
     * Registers a {@link ReindexerPrecept} to the namespace to apply to all items persisted in the namespace.
     *
     * @param namespaceName the namespace name, cannot be {@literal null}
     * @param precept       the {@link ReindexerPrecept} to register, cannot be {@literal null}
     */
    public void registerPrecept(String namespaceName, ReindexerPrecept precept) {
        ReindexerNamespace<?> namespace = getNamespace(namespaceName);
        namespace.registerPrecept(precept);
    }

    private <T> ReindexerNamespace<T> getNamespace(String namespaceName, Class<T> itemClass) {
        ReindexerNamespace<?> namespace = namespaceMap.get(namespaceName);
        if (namespace == null) {
            String msg = String.format("Namespace '%s' is not opened, call openNamespace first", namespaceName);
            throw new IllegalArgumentException(msg);
        }

        if (namespace.getItemClass() != itemClass) {
            throw new RuntimeException("Wrong namespace item type");
        }
        return (ReindexerNamespace<T>) namespace;
    }

    private <T> ReindexerNamespace<T> getNamespace(String namespaceName) {
        ReindexerNamespace<?> namespace = namespaceMap.get(namespaceName);
        if (namespace == null) {
            String msg = String.format("Namespace '%s' is not opened, call openNamespace first", namespaceName);
            throw new IllegalArgumentException(msg);
        }

        return (ReindexerNamespace<T>) namespace;
    }

    @SuppressWarnings("unchecked")
    private <T, E> E modifyItem(String namespaceName, T item, int mode, int format) {
        ReindexerNamespace<E> namespace = getNamespace(namespaceName);
        PreceptExpression[] precepts = namespace.getPrecepts(item, format);
        for (int i = 0; i < 2; i++) {
            try {
                PayloadType payloadType = namespace.getPayloadType();
                int stateToken = payloadType == null ? 0 : payloadType.getStateToken();
                ItemSerializer<T> serializer = ItemSerializer.getInstance(item.getClass(), payloadType);
                byte[] data = serializer.serialize(item);
                QueryResult queryResult = binding.modifyItem(namespace.getName(), data, format, mode, precepts, stateToken);
                updatePayloadType(queryResult, namespace);
                if (precepts.length > 0 && queryResult.getCount() > 0) {
                    return unpackItem(item, queryResult, format, namespace);
                }
                break;
            } catch (StateInvalidatedException e) {
                updatePayloadType(namespace);
            }
        }
        if (format == Consts.FORMAT_C_JSON) {
            return (E) item;
        }
        // No server-generated item was returned for JSON-formatted item.
        return null;
    }

    private void updatePayloadType(QueryResult queryResult, ReindexerNamespace<?> namespace) {
        for (PayloadType payloadType : queryResult.getPayloadTypes()) {
            PayloadType currentPayloadType = namespace.getPayloadType();
            if (currentPayloadType == null || currentPayloadType.getVersion() < payloadType.getVersion()
                    || currentPayloadType.getStateToken() != payloadType.getStateToken()) {
                namespace.updatePayloadType(payloadType);
            }
        }
    }

    private <T> T unpackItem(Object item, QueryResult queryResult, int format, ReindexerNamespace<T> namespace) {
        QueryResult.ItemParams itemParams = queryResult.readItemParams();
        CjsonItemReader<T> reader = createItemReader(item, format, namespace);
        if (queryResult.isWithResultsPtr()) {
            ByteBuffer buffer = NativeUtils.getNativeBuffer(queryResult.getResultsPtr(), itemParams.getCptr(), itemParams.getNsId());
            try {
                return reader.readItem(buffer);
            } finally {
                NativeUtils.freeNativeBuffer(queryResult.getResultsPtr());
            }
        }
        ByteBuffer buffer = queryResult.getBuffer();
        int length = (int) buffer.getUInt32();
        return reader.readItem(new ByteBuffer(buffer.getBytes(length)).rewind());
    }

    @SuppressWarnings("unchecked")
    private <T> CjsonItemReader<T> createItemReader(Object item, int format, ReindexerNamespace<T> namespace) {
        CtagMatcher ctagMatcher = new CtagMatcher();
        PayloadType payloadType = namespace.getPayloadType();
        if (payloadType == null && format == Consts.FORMAT_JSON) {
            // When using JSON, Reindexer does not include payloadType in the response
            // or validate stateToken. Therefore, StateInvalidatedException is never thrown,
            // and payloadType must be explicitly requested to correctly unpack the result.
            updatePayloadType(namespace);
            payloadType = namespace.getPayloadType();
        }
        if (payloadType != null) {
            ctagMatcher.read(payloadType);
        }
        return format == Consts.FORMAT_JSON
                ? new CjsonItemReader<>(namespace.getItemClass(), ctagMatcher)
                : new CjsonItemReader<>(() -> (T) item, namespace.getItemClass(), ctagMatcher);
    }

    private void updatePayloadType(ReindexerNamespace<?> namespace) {
        try {
            query(namespace.getName(), namespace.getItemClass()).limit(0).execute().close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Binding getBinding() {
        return binding;
    }

}

