package ru.rt.restream.reindexer.binding;

import lombok.Data;

/**
 * Result of a query.
 */
@Data
public class QueryResult {

    /**
     * Uniquer request id. Used to fetch query data.
     */
    private final long requestId;

    /**
     * Encoded query result data. Can be decoded with a {@link ru.rt.restream.reindexer.binding.cproto.ByteBuffer}
     * methods
     */
    private final byte[] queryData;

}
