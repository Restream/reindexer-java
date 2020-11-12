package ru.rt.restream.reindexer.connector.options;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class NamespaceOptions {

    public static final NamespaceOptions DEFAULT = NamespaceOptions.builder()
            .enableStorage(true)
            .dropOnIndexConflict(false)
            .dropOnFileFormatError(false)
            .disableObjCache(false)
            .objCacheItemsCount(256000L)
            .build();

    private final boolean enableStorage;

    private final boolean dropOnIndexConflict;

    private final boolean dropOnFileFormatError;

    private final boolean disableObjCache;

    private final long objCacheItemsCount;

}
