package ru.rt.restream.reindexer.connector.options;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class IndexOptions {

    private final boolean isArray;

    private final boolean isAppendable;

    private final boolean isDense;

    private final boolean isPk;

    private final boolean isSparse;

}
