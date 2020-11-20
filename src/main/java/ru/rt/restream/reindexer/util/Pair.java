package ru.rt.restream.reindexer.util;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public final class Pair<FIRST, SECOND> {
    private final FIRST first;
    private final SECOND second;
}
