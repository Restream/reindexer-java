package ru.rt.restream.reindexer.binding.cproto;

import lombok.Builder;
import ru.rt.restream.reindexer.CloseableIterator;
import ru.rt.restream.reindexer.Namespace;
import ru.rt.restream.reindexer.binding.Binding;
import ru.rt.restream.reindexer.binding.QueryResult;
import ru.rt.restream.reindexer.binding.cproto.json.JsonItemReader;
import ru.rt.restream.reindexer.exceptions.UnimplementedException;

/**
 * An iterator over a query result.
 * Maintains a cursor pointing to its current row of data. Initially the cursor is positioned before the first row.
 */
public class CprotoIterator<T> implements CloseableIterator<T> {

    private final Namespace<T> namespace;

    private final Binding binding;

    private final boolean asJson;

    private final int fetchCount;

    private ItemReader<T> itemReader;

    private ByteBuffer buffer;

    private long requestId;

    private long qCount;

    private long count;

    private int position;

    private boolean closed;

    @Builder
    public CprotoIterator(Binding binding,
                          Namespace<T> namespace,
                          QueryResult queryResult,
                          boolean asJson,
                          int fetchCount) {
        this.binding = binding;
        this.namespace = namespace;
        this.asJson = asJson;
        this.fetchCount = fetchCount;
        parseQueryResult(queryResult);
    }

    private void parseQueryResult(QueryResult queryResult) {
        this.requestId = queryResult.getRequestId();
        buffer = queryResult.getBuffer();
        qCount = queryResult.getQCount();
        count += queryResult.getCount();
        long tag = buffer.getVarUInt();
        if (queryResult.isJson()) {
            itemReader = new JsonItemReader<>(namespace.getItemClass());
        } else {
            throw new UnimplementedException();
        }
    }

    @Override
    public boolean hasNext() {
        return position < qCount;
    }

    /**
     * Read next item. Moves the cursor to the next row.
     *
     * @return read item
     * @throws IllegalStateException if the iterator is closed or there is no data to read
     */
    public T next() {
        if (closed) {
            throw new IllegalStateException("Iterator is closed");
        }

        if (!hasNext()) {
            throw new IllegalStateException("No data to read");
        }

        if (needFetch()) {
            fetchResults();
        }

        T item = itemReader.readItem(buffer);

        position++;

        return item;

    }

    private boolean needFetch() {
        return this.position == count;
    }

    private void fetchResults() {
        QueryResult queryResult = binding.fetchResults(requestId, asJson, position, fetchCount);
        parseQueryResult(queryResult);
    }

    /**
     * Closes query results if needed (i.e query request id is not -1).
     */
    @Override
    public void close() {
        if (needClose()) {
            binding.closeResults(requestId);
            requestId = -1L;
            closed = true;
        }
    }

    private boolean needClose() {
        return requestId != -1L;
    }

}
