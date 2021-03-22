/*
 * Copyright 2020 Restream
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.rt.restream.reindexer;

import ru.rt.restream.reindexer.binding.AggregationResult;
import ru.rt.restream.reindexer.binding.QueryResult;
import ru.rt.restream.reindexer.binding.RequestContext;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class QueryResultJsonIterator implements CloseableIterator<String> {

    private final RequestContext requestContext;

    private final int fetchCount;

    private ByteBuffer buffer;

    private QueryResult queryResult;

    private int position;

    private long count;

    private boolean closed;

    public QueryResultJsonIterator(RequestContext requestContext, int fetchCount) {
        this.requestContext = requestContext;
        this.fetchCount = fetchCount;
        parseQueryResult(requestContext.getQueryResult());
    }

    @Override
    public boolean hasNext() {
        return position < queryResult.getQCount();
    }
    /**
     * Read next item as JSON. Moves the cursor to the next row.
     *
     * @return - String with JSON
     * @throws IllegalStateException if the iterator is closed or there is no data to read
     */
    @Override
    public String next() {
        if (closed) {
            throw new IllegalStateException("Iterator is closed");
        }

        if (!hasNext()) {
            throw new IllegalStateException("No data to read");
        }

        if (needFetch()) {
            fetchResults();
        }

        skipItemParams();
        int length = (int) buffer.getUInt32();
        byte[] result = buffer.getBytes(length);

        position++;
        return new String(result, StandardCharsets.UTF_8);

    }

    /**
     * Get Query results in JSON format and close the iterator.
     *
     * @param rootName - Name of root object of output JSON
     * @return - String with all items as JSON
     * @throws IllegalStateException if the iterator is closed or iterator already uses next()
     */
    public String fetchAll(String rootName) {
        if (closed) {
            throw new IllegalStateException("Iterator is closed");
        }

        if (position > 0) {
            throw new IllegalStateException("Iterator already uses next()");
        }

        StringBuilder builder = new StringBuilder()
                .append("{\"")
                .append(rootName)
                .append("\":[");

        if (hasNext()) {
            builder.append(next());
            while (hasNext()) {
                builder.append(',').append(next());
            }
        }

        builder.append("]}");

        this.close();

        return builder.toString();
    }

    @Override
    public long size() {
        return queryResult.getQCount();
    }

    @Override
    public List<AggregationResult> aggResults() {
        return queryResult.getAggResults();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        requestContext.closeResults();
        closed = true;
    }

    private void parseQueryResult(QueryResult queryResult) {
        this.buffer = queryResult.getBuffer();
        this.queryResult = queryResult;
        count += queryResult.getCount();
    }

    private boolean needFetch() {
        return this.position == count;
    }

    private void fetchResults() {
        requestContext.fetchResults(position, fetchCount);
        queryResult = requestContext.getQueryResult();
        parseQueryResult(queryResult);
    }

    private void skipItemParams() {
        if (queryResult.isWithItemId()) {
            buffer.getVarUInt(); // skip Id
            buffer.getVarUInt(); // skip version
        }

        if (queryResult.isWithNsId()) {
            buffer.getVarUInt(); //skipNsId
        }

        // skip rank (used for full-text search)
        if (queryResult.isWithRank()) {
            buffer.getVarUInt();
        }
    }

}
