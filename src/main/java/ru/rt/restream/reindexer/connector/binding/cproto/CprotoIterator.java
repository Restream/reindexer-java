package ru.rt.restream.reindexer.connector.binding.cproto;

import lombok.Builder;
import ru.rt.restream.reindexer.connector.binding.QueryResult;
import ru.rt.restream.reindexer.connector.binding.cproto.json.JsonItemReader;
import ru.rt.restream.reindexer.connector.exceptions.UnimplementedException;

import java.util.Iterator;

public class CprotoIterator<T> implements Iterator<T> {

    private static final int RESULTS_FORMAT_MASK = 0xF;

    private static final int RESULT_JSON = 0x3;

    private final Class<T> itemClass;

    private final Connection connection;

    private ItemReader itemReader;

    private long requestId;

    private int flags;

    private int totalCount;

    private int qCount;

    private int count;

    private int position;

    @Builder
    public CprotoIterator(Connection connection,
                          Class<T> itemClass,
                          QueryResult queryResult) {
        this.connection = connection;
        this.itemClass = itemClass;
        parseQueryResult(queryResult);
    }

    private void parseQueryResult(QueryResult queryResult) {
        this.requestId = queryResult.getRequestId();
        byte[] queryData = queryResult.getQueryData();
        if (queryData.length > 0) {
            ByteBuffer deserializer = new ByteBuffer(queryData);
            this.flags = deserializer.readUnsignedVarInt();
            this.totalCount = deserializer.readUnsignedVarInt();
            this.qCount = deserializer.readUnsignedVarInt();
            this.count = deserializer.readUnsignedVarInt();
            int tag = deserializer.readUnsignedVarInt();
            if ((flags & RESULTS_FORMAT_MASK) == RESULT_JSON) {
                itemReader = new JsonItemReader(deserializer.readBytes());
            } else {
                throw new UnimplementedException();
            }
        }
    }

    @Override
    public boolean hasNext() {
        return position < count;
    }

    public T next() {
        try {
            T item = itemReader.readItem(itemClass);
            position++;
            return item;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
