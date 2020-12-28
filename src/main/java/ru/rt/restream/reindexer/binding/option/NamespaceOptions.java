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
package ru.rt.restream.reindexer.binding.option;

public class NamespaceOptions {

    public static final boolean DEFAULT_DROP_ON_INDEX_CONFLICT = false;

    public static final boolean DEFAULT_ENABLE_STORAGE = true;

    public static final boolean DEFAULT_CREATE_IF_MISSING = true;

    public static final boolean DEFAULT_DROP_ON_FILE_FORMAT_ERROR = false;

    public static final boolean DEFAULT_DISABLE_OBJ_CACHE = false;

    public static final long DEFAULT_OBJ_CACHE_ITEMS_COUNT = 256000L;

    /**
     * Only in memory namespace.
     */
    private boolean enableStorage;

    /**
     * Create item storage if missing.
     * */
    private boolean createStorageIfMissing;
    /**
     * Drop ns on index mismatch error
     */
    private boolean dropOnIndexesConflict;
    /**
     * Drop on file errors
     */
    private boolean dropOnFileFormatError;
    /**
     * Disable object cache
     */
    private boolean disableObjCache;
    /**
     * Object cache items count
     */
    private long objCacheItemsCount;

    public NamespaceOptions(boolean enableStorage, boolean createStorageIfMissing,
                            boolean dropOnIndexesConflict, boolean dropOnFileFormatError,
                            boolean disableObjCache, long objCacheItemsCount) {
        this.enableStorage = enableStorage;
        this.createStorageIfMissing = createStorageIfMissing;
        this.dropOnIndexesConflict = dropOnIndexesConflict;
        this.dropOnFileFormatError = dropOnFileFormatError;
        this.disableObjCache = disableObjCache;
        this.objCacheItemsCount = objCacheItemsCount;
    }

    public static NamespaceOptions defaultOptions() {
        return new NamespaceOptions(DEFAULT_ENABLE_STORAGE, DEFAULT_CREATE_IF_MISSING,
                DEFAULT_DROP_ON_INDEX_CONFLICT, DEFAULT_DROP_ON_FILE_FORMAT_ERROR, DEFAULT_DISABLE_OBJ_CACHE,
                DEFAULT_OBJ_CACHE_ITEMS_COUNT);
    }

    public boolean isEnableStorage() {
        return enableStorage;
    }

    public NamespaceOptions setEnableStorage(boolean enableStorage) {
        this.enableStorage = enableStorage;
        return this;
    }

    public boolean isCreateStorageIfMissing() {
        return createStorageIfMissing;
    }

    public NamespaceOptions setCreateStorageIfMissing(boolean createStorageIfMissing) {
        this.createStorageIfMissing = createStorageIfMissing;
        return this;
    }

    public boolean isDropOnIndexesConflict() {
        return dropOnIndexesConflict;
    }

    public NamespaceOptions setDropOnIndexesConflict(boolean dropOnIndexesConflict) {
        this.dropOnIndexesConflict = dropOnIndexesConflict;
        return this;
    }

    public boolean isDropOnFileFormatError() {
        return dropOnFileFormatError;
    }

    public NamespaceOptions setDropOnFileFormatError(boolean dropOnFileFormatError) {
        this.dropOnFileFormatError = dropOnFileFormatError;
        return this;
    }

    public boolean isDisableObjCache() {
        return disableObjCache;
    }

    public NamespaceOptions setDisableObjCache(boolean disableObjCache) {
        this.disableObjCache = disableObjCache;
        return this;
    }

    public long getObjCacheItemsCount() {
        return objCacheItemsCount;
    }

    public NamespaceOptions setObjCacheItemsCount(long objCacheItemsCount) {
        this.objCacheItemsCount = objCacheItemsCount;
        return this;
    }
}
