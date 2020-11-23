package ru.rt.restream.reindexer;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Builder
@Getter
public class Namespace<T> {

    public static final boolean DEFAULT_DROP_ON_INDEX_CONFLICT = false;

    public static final boolean DEFAULT_ENABLE_STORAGE = true;

    public static final boolean DEFAULT_DROP_ON_FILE_FORMAT_ERROR = false;

    public static final boolean DEFAULT_CREATE_IF_MISSING = false;

    public static final boolean DEFAULT_DISABLE_OBJ_CACHE = false;

    public static final long DEFAULT_OBJ_CACHE_ITEMS_COUNT = 256000L;

    private final String name;

    private final Class<T> itemClass;

    private final boolean enableStorage;

    private final boolean createStorageIfMissing;

    private final boolean dropStorageOnFileFormatError;

    private final boolean dropOnIndexConflict;

    private final boolean disableObjCache;

    private final long objCacheItemsCount;

    private final List<Index> indices;

}
