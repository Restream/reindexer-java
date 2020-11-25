package ru.rt.restream.reindexer.binding;

import lombok.Builder;
import lombok.Data;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;

/**
 * Result of a query.
 */
@Data
@Builder
public class QueryResult {

    /**
     * Used to fetch query data.
     */
    private final long requestId;

    private final long totalCount;

    private final long qCount;

    private final long count;

    private final boolean isJson;

    /**
     * Encoded query result data.
     */
    private final ByteBuffer buffer;

}
