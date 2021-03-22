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

public class ReindexerNamespace<T> implements Namespace<T>{

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

    public static<T> Builder<T> builder() {
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

    public String getName() {
        return name;
    }

    public Class<T> getItemClass() {
        return itemClass;
    }

    public boolean isEnableStorage() {
        return enableStorage;
    }

    public boolean isCreateStorageIfMissing() {
        return createStorageIfMissing;
    }

    public boolean isDropStorageOnFileFormatError() {
        return dropStorageOnFileFormatError;
    }

    public boolean isDropOnIndexConflict() {
        return dropOnIndexConflict;
    }


    public boolean isDisableObjCache() {
        return disableObjCache;
    }


    public long getObjCacheItemsCount() {
        return objCacheItemsCount;
    }


    public List<ReindexerIndex> getIndexes() {
        return indexes;
    }

    public String[] getPrecepts() {
        return precepts;
    }

    public PayloadType getPayloadType() {
        return payloadType;
    }

    public synchronized void updatePayloadType(PayloadType payloadType) {
        if (this.payloadType == null || this.payloadType.getVersion() < payloadType.getVersion()) {
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

        public Builder<T> name(String name) {
            this.name = name;
            return this;
        }

        public Builder<T> itemClass(Class<T> itemClass) {
            this.itemClass = itemClass;
            return this;
        }

        public Builder<T> enableStorage(boolean enableStorage) {
            this.enableStorage = enableStorage;
            return this;
        }

        public Builder<T> createStorageIfMissing(boolean createStorageIfMissing) {
            this.createStorageIfMissing = createStorageIfMissing;
            return this;
        }

        public Builder<T> dropStorageOnFileFormatError(boolean dropStorageOnFileFormatError) {
            this.dropStorageOnFileFormatError = dropStorageOnFileFormatError;
            return this;
        }

        public Builder<T> dropOnIndexConflict(boolean dropOnIndexConflict) {
            this.dropOnIndexConflict = dropOnIndexConflict;
            return this;
        }

        public Builder<T> disableObjCache(boolean disableObjCache) {
            this.disableObjCache = disableObjCache;
            return this;
        }

        public Builder<T> objCacheItemsCount(long objCacheItemsCount) {
            this.objCacheItemsCount = objCacheItemsCount;
            return this;
        }

        public Builder<T> indexes(List<ReindexerIndex> indexes) {
            this.indexes = indexes;
            return this;
        }

        public Builder<T> reindexer(Reindexer reindexer) {
            this.reindexer = reindexer;
            return this;
        }

        public ReindexerNamespace<T> build() {
            return new ReindexerNamespace<>(this);
        }
    }
}
