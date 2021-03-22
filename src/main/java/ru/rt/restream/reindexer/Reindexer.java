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
import ru.rt.restream.reindexer.annotations.ReindexAnnotationScanner;
import ru.rt.restream.reindexer.binding.Binding;
import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.binding.QueryResult;
import ru.rt.restream.reindexer.binding.RequestContext;
import ru.rt.restream.reindexer.binding.cproto.ItemSerializer;
import ru.rt.restream.reindexer.binding.cproto.cjson.ItemSerializerFactory;
import ru.rt.restream.reindexer.binding.cproto.cjson.PayloadType;
import ru.rt.restream.reindexer.binding.definition.IndexDefinition;
import ru.rt.restream.reindexer.binding.definition.NamespaceDefinition;
import ru.rt.restream.reindexer.binding.option.NamespaceOptions;
import ru.rt.restream.reindexer.exceptions.IndexConflictException;
import ru.rt.restream.reindexer.exceptions.StateInvalidatedException;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class Reindexer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Reindexer.class);

    static final int MODE_UPDATE = 0;

    static final int MODE_INSERT = 1;

    static final int MODE_UPSERT = 2;

    static final int MODE_DELETE = 3;

    private final Binding binding;

    private final ReindexScanner reindexScanner = new ReindexAnnotationScanner();

    private final ItemSerializerFactory serializerFactory = new ItemSerializerFactory();

    protected final Map<String, ReindexerNamespace<?>> namespaceMap = new ConcurrentHashMap<>();

    protected Reindexer(Binding binding) {
        this.binding = binding;
    }

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
     */
    public <T> void insert(String namespaceName, T item) {
        modifyItem(namespaceName, item, MODE_INSERT, Consts.FORMAT_C_JSON);
    }

    /**
     * Inserts the given json-formatted item data.
     *
     * @param namespaceName the namespace name
     * @param json          the json-formatted item data
     */
    public void insert(String namespaceName, String json) {
        modifyItem(namespaceName, json, MODE_INSERT, Consts.FORMAT_JSON);
    }

    /**
     * Updates the given item data.
     *
     * @param <T>           the item type
     * @param namespaceName the namespace name
     * @param item          the item data
     */
    public <T> void update(String namespaceName, T item) {
        modifyItem(namespaceName, item, MODE_UPDATE, Consts.FORMAT_C_JSON);
    }

    /**
     * Updates the given json-formatted item data.
     *
     * @param namespaceName the namespace name
     * @param json          the json-formatted item data
     */
    public void update(String namespaceName, String json) {
        modifyItem(namespaceName, json, MODE_UPDATE, Consts.FORMAT_JSON);
    }

    /**
     * Inserts or updates the given item data.
     *
     * @param <T>           the item type
     * @param namespaceName the namespace name
     * @param item          the item data
     */
    public <T> void upsert(String namespaceName, T item) {
        modifyItem(namespaceName, item, MODE_UPSERT, Consts.FORMAT_C_JSON);
    }

    /**
     * Inserts or updates the given json-formatted item data.
     *
     * @param namespaceName the namespace name
     * @param json          the json-formatted item data
     */
    public void upsert(String namespaceName, String json) {
        modifyItem(namespaceName, json, MODE_UPSERT, Consts.FORMAT_JSON);
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
            if ("FROM".equals(word)) {
                namespaceName = words[++i];
                break;
            }
        }
        if (namespaceName == null) {
            throw new RuntimeException("Invalid select query, namespace name not found");
        }
        ReindexerNamespace<T> namespace = getNamespace(namespaceName, itemClass);
        long [] ptVersions = new long[] {namespace.getPayloadType().getVersion()};
        RequestContext ctx = binding.select(query, false, Integer.MAX_VALUE, ptVersions);
        QueryResult queryResult = ctx.getQueryResult();
        for (PayloadType payloadType : queryResult.getPayloadTypes()) {
            PayloadType currentPayloadType = namespace.getPayloadType();
            if (currentPayloadType == null || currentPayloadType.getVersion() < payloadType.getVersion()) {
                namespace.updatePayloadType(payloadType);
            }
        }
        return new QueryResultIterator<>(namespace, ctx, null, Integer.MAX_VALUE);
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
    private <T> void modifyItem(String namespaceName, T item, int mode, int itemFormat) {
        ReindexerNamespace<?> namespace = getNamespace(namespaceName);
        String[] percepts = namespace.getPrecepts();
        for (int i = 0; i < 2; i++) {
            try {
                PayloadType payloadType = namespace.getPayloadType();
                int stateToken = payloadType == null ? 0 : payloadType.getStateToken();
                ItemSerializer<T> serializer = serializerFactory.get(item.getClass(), payloadType);
                byte[] data = serializer.serialize(item);
                binding.modifyItem(namespace.getName(), data, itemFormat, mode, percepts, stateToken);
                break;
            } catch (StateInvalidatedException e) {
                updatePayloadType(namespace);
            }
        }
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

