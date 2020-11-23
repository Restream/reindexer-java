package ru.rt.restream.reindexer;

import lombok.Builder;
import lombok.Getter;
import ru.rt.restream.reindexer.util.Pair;

import java.util.List;
import java.util.Set;

@Builder
@Getter
public class Index {

    public enum Option {
        PK, DENSE, SPARSE, APPENDABLE, ARRAY, LINEAR
    }

    private final Pair<String, String> indexFieldPair;

    private final List<String> jsonPaths;

    private final String indexType;

    private final String fieldType;

    private final Set<Option> options;

    private final String collateMode;

    private final String sortOrder;

}
