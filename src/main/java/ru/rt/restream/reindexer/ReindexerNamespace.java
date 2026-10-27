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

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.binding.cproto.cjson.PayloadType;
import ru.rt.restream.reindexer.expression.Expression;
import ru.rt.restream.reindexer.expression.PreceptExpression;
import ru.rt.restream.reindexer.util.BeanPropertyUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

/**
 * Contains the reindexer namespace configuration and methods for manipulating the linked reindexer namespace data.
 */
@Slf4j
@Getter
public class ReindexerNamespace<T> implements Namespace<T> {

    /**
     * Read/Write lock.
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    @Getter(value = AccessLevel.NONE)
    private final Reindexer reindexer;

    /**
     * Namespace's name.
     */
    private final String name;

    /**
     * Class of objects that are stored in the current namespace.
     */
    private final Class<T> itemClass;

    /**
     * An indication, that storage is enabled for the current namespace.
     */
    private final boolean enableStorage;

    /**
     * Indication, that the namespace storage will be created if not exists.
     */
    private final boolean createStorageIfMissing;

    /**
     * Indication, that the namespace storage will be dropped on file format error.
     */
    private final boolean dropStorageOnFileFormatError;

    /**
     * Indication, that the namespace will be dropped on index conflict.
     */
    private final boolean dropOnIndexConflict;

    /**
     * Indication, that the namespace object cache is disabled.
     */
    private final boolean disableObjCache;

    /**
     * Object cache item count.
     */
    private final long objCacheItemsCount;

    /**
     * List of namespace indexes.
     */
    private final List<ReindexerIndex> indexes;

    /**
     * List of namespace precepts.
     *
     * <p>Precept is a special reindexer embedded function, such as serial(), now().
     */
    @Getter(AccessLevel.NONE)
    private final Map<String, ReindexerPrecept> precepts = new HashMap<>();

    /**
     * Current namespace payload type.
     *
     * <p>It is an item descriptor that contains current item type
     * state - fields, tags, version and namespace information.
     */
    @Getter
    private volatile PayloadType payloadType;

    /**
     * Get the reindexer namespace builder object.
     *
     * @return the namespace builder object
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    private ReindexerNamespace(Builder<T> builder) {
        this.itemClass = builder.itemClass;
        this.name = builder.name;
        this.enableStorage = builder.enableStorage;
        this.createStorageIfMissing = builder.createStorageIfMissing;
        this.dropOnIndexConflict = builder.dropOnIndexConflict;
        this.dropStorageOnFileFormatError = builder.dropStorageOnFileFormatError;
        this.disableObjCache = builder.disableObjCache;
        this.objCacheItemsCount = builder.objCacheItemsCount;
        this.indexes = builder.indexes;
        this.reindexer = builder.reindexer;
    }

    void initialize() {
        // Initialize precepts.
        for (ReindexerIndex index : indexes) {
            ReindexerPrecept precept = index.getPrecept();
            if (precept != null) {
                registerPrecept(precept);
            }
        }
    }

    /**
     * Update the current namespace payload type. If current payload type version is lower than passed or payload type
     * state token is lower than passed. Payload type will be updated.
     *
     * @param payloadType new payload object
     */
    public synchronized void updatePayloadType(PayloadType payloadType) {
        if (this.payloadType == null || this.payloadType.getVersion() < payloadType.getVersion()
                || this.payloadType.getStateToken() != payloadType.getStateToken()) {
            this.payloadType = payloadType;
        }
    }

    @Override
    public Transaction<T> beginTransaction() {
        return reindexer.beginTransaction(name, itemClass);
    }

    @Override
    public T insert(T item) {
        return reindexer.insert(name, item);
    }

    @Override
    public Optional<T> insert(String item) {
        return reindexer.insert(name, item);
    }

    @Override
    public T upsert(T item) {
        return reindexer.upsert(name, item);
    }

    @Override
    public Optional<T> upsert(String item) {
        return reindexer.upsert(name, item);
    }

    @Override
    public T update(T item) {
        return reindexer.update(name, item);
    }

    @Override
    public Optional<T> update(String item) {
        return reindexer.update(name, item);
    }

    @Override
    public void delete(T item) {
        reindexer.delete(name, item);
    }

    @Override
    public void delete(String item) {
        reindexer.delete(name, item);
    }

    @Override
    public Query<T> query() {
        return reindexer.query(name, itemClass);
    }

    @Override
    public void putMeta(String key, String data) {
        reindexer.getBinding().putMeta(name, key, data);
    }

    @Override
    public String getMeta(String key) {
        return reindexer.getBinding().getMeta(name, key);
    }

    @Override
    public ResultIterator<T> execSql(String query) {
        return reindexer.execSql(query, itemClass);
    }

    @Override
    public void updateSql(String query) {
        reindexer.updateSql(query);
    }

    void registerPrecept(ReindexerPrecept precept) {
        Objects.requireNonNull(precept, "precept cannot be null");
        lock.writeLock().lock();
        try {
            ReindexerPrecept existing = precepts.put(precept.getJsonPath(), precept);
            if (existing == null) {
                log.trace("Registered precept {} to namespace {}", precept, name);
            } else {
                log.trace("Precept {} was overridden to {} for namespace {}", existing, precept, name);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    PreceptExpression[] getPrecepts(Object item, int format) {
        Predicate<ReindexerPrecept> isNullPredicate = getIsNullPredicate(item, format);
        List<PreceptExpression> result = new ArrayList<>();
        lock.readLock().lock();
        try {
            for (ReindexerPrecept precept : precepts.values()) {
                applyPrecept(precept, isNullPredicate, result);
            }
        } finally {
            lock.readLock().unlock();
        }
        return result.toArray(new PreceptExpression[0]);
    }

    private Predicate<ReindexerPrecept> getIsNullPredicate(Object item, int format) {
        if (format == Consts.FORMAT_JSON) {
            Configuration configuration = Configuration.defaultConfiguration()
                    .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
            DocumentContext document = JsonPath.using(configuration).parse((String) item);
            // Lookup for a field value using json-path e.g., "$.id".
            return it -> document.read("$." + it.getJsonPath()) == null;
        }
        // Lookup for a field value using a field name.
        return it -> BeanPropertyUtils.getProperty(item, it.getFieldPath()) == null;
    }

    private void applyPrecept(ReindexerPrecept precept, Predicate<ReindexerPrecept> isNullPredicate,
                              List<PreceptExpression> precepts) {
        // Apply precept according to the strategy.
        switch (precept.getStrategy()) {
            case ALWAYS:
                // Apply precept to all items.
                precepts.add(toPreceptExpression(precept));
                break;
            case WHEN_NULL:
                // Apply precept only to items that have null value for the specified field.
                if (isNullPredicate.test(precept)) {
                    precepts.add(toPreceptExpression(precept));
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown apply strategy: " + precept.getStrategy());
        }
    }

    private PreceptExpression toPreceptExpression(ReindexerPrecept precept) {
        return Expression.string(precept.getJsonPath() + '=' + precept.getExpression());
    }

    /**
     * Reindexer namespace builder.
     */
    public static final class Builder<T> {
        private String name;
        private Class<T> itemClass;
        private boolean enableStorage;
        private boolean createStorageIfMissing;
        private boolean dropStorageOnFileFormatError;
        private boolean dropOnIndexConflict;
        private boolean disableObjCache;
        private long objCacheItemsCount;
        private List<ReindexerIndex> indexes;
        public Reindexer reindexer;

        private Builder() {
        }

        /**
         * Set the namespace name.
         *
         * @param name the namespace name
         * @return this {@link Builder} for further customization
         */
        public Builder<T> name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Set the namespace item type.
         *
         * @param itemClass the namespace item class
         * @return this {@link Builder} for further customization
         */
        public Builder<T> itemClass(Class<T> itemClass) {
            this.itemClass = itemClass;
            return this;
        }

        /**
         * Enable namespace external storage.
         *
         * @param enableStorage true, if external storage enabled
         * @return this {@link Builder} for further customization
         */
        public Builder<T> enableStorage(boolean enableStorage) {
            this.enableStorage = enableStorage;
            return this;
        }

        /**
         * Create external storage if not exists.
         *
         * @param createStorageIfMissing true, if external storage should be created if not exists
         * @return this {@link Builder} for further customization
         */
        public Builder<T> createStorageIfMissing(boolean createStorageIfMissing) {
            this.createStorageIfMissing = createStorageIfMissing;
            return this;
        }

        /**
         * Drop external storage on file format error.
         *
         * @param dropStorageOnFileFormatError true, if external storage should be dropped on file format error
         * @return this {@link Builder} for further customization
         */
        public Builder<T> dropStorageOnFileFormatError(boolean dropStorageOnFileFormatError) {
            this.dropStorageOnFileFormatError = dropStorageOnFileFormatError;
            return this;
        }

        /**
         * Drop namespace on index conflict.
         *
         * @param dropOnIndexConflict true, if namespace should be dropped on index conflict
         * @return this {@link Builder} for further customization
         */
        public Builder<T> dropOnIndexConflict(boolean dropOnIndexConflict) {
            this.dropOnIndexConflict = dropOnIndexConflict;
            return this;
        }

        /**
         * Disable namespace object cache
         *
         * @param disableObjCache true, if object cache is disabled
         * @return this {@link Builder} for further customization
         */
        public Builder<T> disableObjCache(boolean disableObjCache) {
            this.disableObjCache = disableObjCache;
            return this;
        }

        /**
         * Set the object cache item count.
         *
         * @param objCacheItemsCount the object cache size
         * @return this {@link Builder} for further customization
         */
        public Builder<T> objCacheItemsCount(long objCacheItemsCount) {
            this.objCacheItemsCount = objCacheItemsCount;
            return this;
        }

        /**
         * Set the namespace indexes.
         *
         * @param indexes the namespace indexes
         * @return this {@link Builder} for further customization
         */
        public Builder<T> indexes(List<ReindexerIndex> indexes) {
            this.indexes = indexes;
            return this;
        }

        /**
         * Bind namespace to the {@link Reindexer} object.
         *
         * @param reindexer the {@link Reindexer} instance to bind a namespace
         */
        public Builder<T> reindexer(Reindexer reindexer) {
            this.reindexer = reindexer;
            return this;
        }

        /**
         * Build a new {@link ReindexerNamespace object} with a builder.
         *
         * @return the new {@link ReindexerNamespace} instance
         */
        public ReindexerNamespace<T> build() {
            return new ReindexerNamespace<>(this);
        }
    }
}
