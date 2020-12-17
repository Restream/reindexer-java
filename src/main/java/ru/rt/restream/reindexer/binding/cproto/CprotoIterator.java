package ru.rt.restream.reindexer.binding.cproto;

import ru.rt.restream.reindexer.CloseableIterator;
import ru.rt.restream.reindexer.ReindexerNamespace;
import ru.rt.restream.reindexer.binding.Binding;
import ru.rt.restream.reindexer.binding.QueryResult;
import ru.rt.restream.reindexer.binding.cproto.cjson.CjsonItemReader;
import ru.rt.restream.reindexer.binding.cproto.json.JsonItemReader;

/**
 * An iterator over a query result.
 * Maintains a cursor pointing to its current row of data. Initially the cursor is positioned before the first row.
 */
public class CprotoIterator<T> implements CloseableIterator<T> {

    private final ReindexerNamespace<T> namespace;

    private final Binding binding;

    private final int fetchCount;

    private ItemReader<T> itemReader;

    private ByteBuffer buffer;

    private QueryResult queryResult;

    private int position;

    private long count;

    private boolean closed;

    public CprotoIterator(Binding binding,
                          ReindexerNamespace<T> namespace,
                          QueryResult queryResult,
                          int fetchCount) {
        this.binding = binding;
        this.namespace = namespace;
        this.fetchCount = fetchCount;
        parseQueryResult(queryResult);
    }

    private void parseQueryResult(QueryResult queryResult) {
        this.buffer = queryResult.getBuffer();
        this.queryResult = queryResult;
        count += queryResult.getCount();
        if (itemReader == null) {
            if (queryResult.isJson()) {
                itemReader = new JsonItemReader<>(namespace.getItemClass(), queryResult.isWithRank());
            } else {
                itemReader = new CjsonItemReader<>(namespace.getItemClass(), queryResult.getPayloadTypes().get(0));
            }
        }
    }

    @Override
    public boolean hasNext() {
        return position < queryResult.getQCount();
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

        readItemParams();
        int length = (int) buffer.getUInt32();
        T item = itemReader.readItem(new ByteBuffer(buffer.getBytes(length)).rewind());

        position++;

        return item;

    }

    private void readItemParams() {
        long rank = -1;
        long id = -1;
        long version = -1;
        long nsId = -1;

        if (queryResult.isWithItemId()) {
            id = buffer.getVarUInt();
            version = buffer.getVarUInt();
        }

        if (queryResult.isWithNsId()) {
            nsId = buffer.getVarUInt();
        }

        if (queryResult.isWithRank()) {
            //used for full-text search
            rank = buffer.getVarUInt();
        }
    }

    private boolean needFetch() {
        return this.position == count;
    }

    private void fetchResults() {
        QueryResult queryResult = binding.fetchResults(this.queryResult.getRequestId(), this.queryResult.isJson(),
                position, fetchCount);
        parseQueryResult(queryResult);
    }

    /**
     * Closes query results if needed (i.e query request id is not -1).
     */
    @Override
    public void close() {
        if (needClose()) {
            binding.closeResults(this.queryResult.getRequestId());
            this.queryResult.setRequestId(-1);
            closed = true;
        }
    }

    private boolean needClose() {
        return this.queryResult.getRequestId() != -1L;
    }

}
