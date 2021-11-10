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

import ru.rt.restream.reindexer.binding.cproto.cjson.PayloadType;

import java.util.List;
import java.util.Objects;

/**
 * Contains the reindexer namespace configuration and methods for manipulating the linked reindexer namespace data.
 */
public class ReindexerNamespace<T> implements Namespace<T> {

    private final String name;

    private final Class<T> itemClass;

    private final boolean enableStorage;

    private final boolean createStorageIfMissing;

    private final boolean dropStorageOnFileFormatError;

    private final boolean dropOnIndexConflict;

    private final boolean disableObjCache;

    private final long objCacheItemsCount;

    private final List<ReindexerIndex> indexes;

    private final String[] precepts;

    private final Reindexer reindexer;

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
        this.precepts = indexes.stream()
                .map(ReindexerIndex::getPrecept)
                .filter(Objects::nonNull)
                .toArray(String[]::new);
        this.reindexer = builder.reindexer;
    }

    /**
     * Get the current namespace name.
     *
     * @return the current namespace name
     */
    public String getName() {
        return name;
    }

    /**
     * Get the class of objects that are stored in the current namespace.
     *
     * @return the current namespace item class
     */
    public Class<T> getItemClass() {
        return itemClass;
    }

    /**
     * An indication, that storage is enabled for the current namespace.
     *
     * @return true, if external storage enabled
     */
    public boolean isEnableStorage() {
        return enableStorage;
    }

    /**
     * Get the indication, that the namespace storage will be created if not exists.
     *
     * @return true, if external storage will be created if not exists
     */
    public boolean isCreateStorageIfMissing() {
        return createStorageIfMissing;
    }

    /**
     * Get the indication, that the namespace storage will be dropped on file format error.
     *
     * @return true, if external storage will be dropped on file format error
     */
    public boolean isDropStorageOnFileFormatError() {
        return dropStorageOnFileFormatError;
    }

    /**
     * Get the indication, that the namespace will be dropped on index conflict.
     *
     * @return true, if namespace will be dropped on index conflict
     */
    public boolean isDropOnIndexConflict() {
        return dropOnIndexConflict;
    }

    /**
     * Get the indication, that the namespace object cache is disabled.
     *
     * @return true, if object cache is disabled
     */
    public boolean isDisableObjCache() {
        return disableObjCache;
    }

    /**
     * Get the object cache item count.
     *
     * @return the object cache item count
     */
    public long getObjCacheItemsCount() {
        return objCacheItemsCount;
    }


    /**
     * Get the list of namespace indexes. {@link ReindexerIndex}
     *
     * @return list of namespace indexes
     */
    public List<ReindexerIndex> getIndexes() {
        return indexes;
    }

    /**
     * Get the list of namespace precepts. Precept is a special reindexer embedded function, such as serial(), now().
     *
     * @return the namespace precepts
     */
    public String[] getPrecepts() {
        return precepts;
    }

    /**
     * Get the current namespace payload type. {@link PayloadType} is a item descriptor that contains current item type
     * state - fields, tags, version and namespace information.
     *
     * @return the namespace item payload type
     */
    public PayloadType getPayloadType() {
        return payloadType;
    }

    /**
     * Update the current namespace payload type. If current payload type version is lower than passed or payload type
     * state token is lower than passed. Payload type will be updated.
     *
     * @param payloadType new payload object
     */
    public synchronized void updatePayloadType(PayloadType payloadType) {
        if (this.payloadType == null || this.payloadType.getVersion() < payloadType.getVersion()
            || this.payloadType.getStateToken() < payloadType.getStateToken()) {
            this.payloadType = payloadType;
        }
    }

    @Override
    public Transaction<T> beginTransaction() {
        return reindexer.beginTransaction(name, itemClass);
    }

    @Override
    public void insert(T item) {
        reindexer.insert(name, item);
    }

    @Override
    public void insert(String item) {
        reindexer.insert(name, item);
    }

    @Override
    public void upsert(T item) {
        reindexer.upsert(name, item);
    }

    @Override
    public void upsert(String item) {
        reindexer.upsert(name, item);
    }

    @Override
    public void update(T item) {
        reindexer.update(name, item);
    }

    @Override
    public void update(String item) {
        reindexer.update(name, item);
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
    public CloseableIterator<T> execSql(String query) {
        return reindexer.execSql(query, itemClass);
    }

    @Override
    public void updateSql(String query) {
        reindexer.updateSql(query);
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
