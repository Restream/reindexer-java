package ru.rt.restream.reindexer.binding.definition;

import lombok.Builder;
import lombok.Getter;
import ru.rt.restream.reindexer.Index;

import java.util.List;
import java.util.Set;

import static ru.rt.restream.reindexer.Index.Option.APPENDABLE;
import static ru.rt.restream.reindexer.Index.Option.ARRAY;
import static ru.rt.restream.reindexer.Index.Option.DENSE;
import static ru.rt.restream.reindexer.Index.Option.LINEAR;
import static ru.rt.restream.reindexer.Index.Option.PK;
import static ru.rt.restream.reindexer.Index.Option.SPARSE;

/**
 * Data-transfer object class, which is used to create namespace index.
 */
@Builder
@Getter
public class IndexDefinition {

    public static IndexDefinition fromIndex(Index index) {
        Set<Index.Option> options = index.getOptions();

        return IndexDefinition.builder()
                .name(index.getIndexFieldPair().getFirst())
                .collateMode(index.getCollateMode())
                .fieldType(index.getFieldType())
                .indexType(index.getIndexType())
                .isArray(options.contains(ARRAY))
                .isDense(options.contains(DENSE))
                .isPk(options.contains(PK))
                .isSparse(options.contains(SPARSE))
                .isLinear(options.contains(LINEAR))
                .isAppendable(options.contains(APPENDABLE))
                .jsonPaths(index.getJsonPaths())
                .sortOrder(index.getSortOrder())
                .build();
    }

    private final String name;

    private final List<String> jsonPaths;

    private final String indexType;

    private final String fieldType;

    private final boolean isPk;

    private final boolean isArray;

    private final boolean isDense;

    private final boolean isSparse;

    private final boolean isLinear;

    private final boolean isAppendable;

    private final String collateMode;

    private final String sortOrder;

}
