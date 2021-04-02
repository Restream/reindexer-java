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

/**
 * The namespace options.
 */
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
     */
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

    /**
     * Creates default namespace options.
     *
     * @return {@link NamespaceOptions} with default values
     */
    public static NamespaceOptions defaultOptions() {
        return new NamespaceOptions(DEFAULT_ENABLE_STORAGE, DEFAULT_CREATE_IF_MISSING,
                DEFAULT_DROP_ON_INDEX_CONFLICT, DEFAULT_DROP_ON_FILE_FORMAT_ERROR, DEFAULT_DISABLE_OBJ_CACHE,
                DEFAULT_OBJ_CACHE_ITEMS_COUNT);
    }

    /**
     * Get the indication that namespace storage enabled.
     *
     * @return true, if external storage enabled
     */
    public boolean isEnableStorage() {
        return enableStorage;
    }

    /**
     * Set the indication that namespace storage enabled.
     *
     * @param enableStorage true, if external storage enabled
     * @return this {@link NamespaceOptions} for further customization
     */
    public NamespaceOptions setEnableStorage(boolean enableStorage) {
        this.enableStorage = enableStorage;
        return this;
    }

    /**
     * Get the indication, that the namespace storage will be dropped on file format error.
     *
     * @return true, if namespace storage will be dropped on file format error
     */
    public boolean isCreateStorageIfMissing() {
        return createStorageIfMissing;
    }

    /**
     * Set the indication, that the namespace storage will be created if not exists.
     *
     * @param createStorageIfMissing true, if namespace storage should be created if not exists.
     * @return this {@link NamespaceOptions} for further customization
     */
    public NamespaceOptions setCreateStorageIfMissing(boolean createStorageIfMissing) {
        this.createStorageIfMissing = createStorageIfMissing;
        return this;
    }

    /**
     * Get the indication, that the namespace will be dropped on index conflict.
     *
     * @return true, if namespace should be dropped on index conflict
     */
    public boolean isDropOnIndexesConflict() {
        return dropOnIndexesConflict;
    }

    /**
     * Set the indication, that the namespace will be dropped on index conflict.
     *
     * @param dropOnIndexesConflict true, if the namespace needs to be dropped on an index conflict
     * @return this {@link NamespaceOptions} for further customization
     */
    public NamespaceOptions setDropOnIndexesConflict(boolean dropOnIndexesConflict) {
        this.dropOnIndexesConflict = dropOnIndexesConflict;
        return this;
    }

    /**
     * Get the indication, that the namespace storage will be dropped on file format error.
     *
     * @return true, if namespace storage should be dropped on file fornat error
     */
    public boolean isDropOnFileFormatError() {
        return dropOnFileFormatError;
    }

    /**
     * Set the indication, that the namespace storage will be dropped on file format error.
     *
     * @param dropOnFileFormatError true, if the namespace storage needs to be dropped on a file format error
     * @return this {@link NamespaceOptions} for further customization
     */
    public NamespaceOptions setDropOnFileFormatError(boolean dropOnFileFormatError) {
        this.dropOnFileFormatError = dropOnFileFormatError;
        return this;
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
     * Set the indication, that the namespace object cache is disabled.
     *
     * @param disableObjCache true, if object cache is disabled
     * @return this {@link NamespaceOptions} for further customization
     */
    public NamespaceOptions setDisableObjCache(boolean disableObjCache) {
        this.disableObjCache = disableObjCache;
        return this;
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
     * Set the object cache item count.
     *
     * @param objCacheItemsCount object cache item count
     * @return this {@link NamespaceOptions} for further customization
     */
    public NamespaceOptions setObjCacheItemsCount(long objCacheItemsCount) {
        this.objCacheItemsCount = objCacheItemsCount;
        return this;
    }
}
