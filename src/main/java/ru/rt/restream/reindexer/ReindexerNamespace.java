package ru.rt.restream.reindexer;

import java.util.List;

public class ReindexerNamespace<T> {

    private String name;

    private Class<T> itemClass;

    private boolean enableStorage;

    private boolean createStorageIfMissing;

    private boolean dropStorageOnFileFormatError;

    private boolean dropOnIndexConflict;

    private boolean disableObjCache;

    private long objCacheItemsCount;

    private List<ReindexerIndex> indexes;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Class<T> getItemClass() {
        return itemClass;
    }

    public void setItemClass(Class<T> itemClass) {
        this.itemClass = itemClass;
    }

    public boolean isEnableStorage() {
        return enableStorage;
    }

    public void setEnableStorage(boolean enableStorage) {
        this.enableStorage = enableStorage;
    }

    public boolean isCreateStorageIfMissing() {
        return createStorageIfMissing;
    }

    public void setCreateStorageIfMissing(boolean createStorageIfMissing) {
        this.createStorageIfMissing = createStorageIfMissing;
    }

    public boolean isDropStorageOnFileFormatError() {
        return dropStorageOnFileFormatError;
    }

    public void setDropStorageOnFileFormatError(boolean dropStorageOnFileFormatError) {
        this.dropStorageOnFileFormatError = dropStorageOnFileFormatError;
    }

    public boolean isDropOnIndexConflict() {
        return dropOnIndexConflict;
    }

    public void setDropOnIndexConflict(boolean dropOnIndexConflict) {
        this.dropOnIndexConflict = dropOnIndexConflict;
    }

    public boolean isDisableObjCache() {
        return disableObjCache;
    }

    public void setDisableObjCache(boolean disableObjCache) {
        this.disableObjCache = disableObjCache;
    }

    public long getObjCacheItemsCount() {
        return objCacheItemsCount;
    }

    public void setObjCacheItemsCount(long objCacheItemsCount) {
        this.objCacheItemsCount = objCacheItemsCount;
    }

    public List<ReindexerIndex> getIndexes() {
        return indexes;
    }

    public void setIndexes(List<ReindexerIndex> indexes) {
        this.indexes = indexes;
    }
}
