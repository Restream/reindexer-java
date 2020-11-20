package ru.rt.restream.reindexer.connector.binding;

import lombok.Data;

@Data
public class QueryResult {

    private final long requestId;

    private final byte[] queryData;

}
