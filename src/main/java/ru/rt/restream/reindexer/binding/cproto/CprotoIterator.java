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
package ru.rt.restream.reindexer.binding.cproto;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.rt.restream.reindexer.binding.AggregationResult;
import ru.rt.restream.reindexer.CloseableIterator;
import ru.rt.restream.reindexer.Query;
import ru.rt.restream.reindexer.ReindexerNamespace;
import ru.rt.restream.reindexer.Transaction;
import ru.rt.restream.reindexer.annotations.Transient;
import ru.rt.restream.reindexer.binding.QueryResult;
import ru.rt.restream.reindexer.binding.RequestContext;
import ru.rt.restream.reindexer.binding.cproto.cjson.CjsonItemReader;
import ru.rt.restream.reindexer.binding.cproto.cjson.CtagMatcher;
import ru.rt.restream.reindexer.binding.cproto.cjson.PayloadType;
import ru.rt.restream.reindexer.util.BeanPropertyUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.List;

/**
 * An iterator over a query result.
 * Maintains a cursor pointing to its current row of data. Initially the cursor is positioned before the first row.
 */
public class CprotoIterator<T> implements CloseableIterator<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Transaction.class);

    private final ReindexerNamespace<T> namespace;

    private final RequestContext requestContext;

    private final int fetchCount;

    private ItemReader<T> itemReader;

    private ByteBuffer buffer;

    private QueryResult queryResult;

    private Query<T> query;

    private int position;

    private long count;

    private boolean closed;

    public CprotoIterator(ReindexerNamespace<T> namespace,
                          RequestContext requestContext,
                          Query<T> query,
                          int fetchCount) {
        this.namespace = namespace;
        this.requestContext = requestContext;
        this.fetchCount = fetchCount;
        this.query = query;
        parseQueryResult(requestContext.getQueryResult());
    }

    private void parseQueryResult(QueryResult queryResult) {
        this.buffer = queryResult.getBuffer();
        this.queryResult = queryResult;
        count += queryResult.getCount();
        if (itemReader == null) {
            if (queryResult.isJson()) {
                throw new UnsupportedOperationException("Query result in json format is not supported");
            } else {
                CtagMatcher ctagMatcher = new CtagMatcher();
                PayloadType payloadType = namespace.getPayloadType();
                if (payloadType != null) {
                    ctagMatcher.read(payloadType);
                }
                itemReader = new CjsonItemReader<>(namespace.getItemClass(), ctagMatcher);
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

        ItemParams params = readItemParams();
        int length = (int) buffer.getUInt32();
        T item = itemReader.readItem(new ByteBuffer(buffer.getBytes(length)).rewind());

        long subNsRes = -1L;
        if (queryResult.isWithJoined()) {
            subNsRes = buffer.getVarUInt();
        }

        int nsIndexOffset = getJoinedNsIndexOffset(params.nsId);

        Map<String, List<Object>> subItemsMap = new HashMap<>();
        for (int nsIndex = 0; nsIndex < subNsRes; nsIndex++) {
            List<ReindexerNamespace<?>> namespaces = query.getNamespaces();
            ReindexerNamespace<?> subItemNamespace =  namespaces.get(nsIndex + nsIndexOffset);
            CtagMatcher ctagMatcher = new CtagMatcher();
            PayloadType subItemPayloadType = subItemNamespace.getPayloadType();
            ctagMatcher.read(subItemPayloadType);
            Class<?> siClass = subItemNamespace.getItemClass();
            CjsonItemReader<?> subItemItemReader = new CjsonItemReader<>(siClass, ctagMatcher);
            String joinField =  query.getJoinFields().get(nsIndex);
            List<Object> subItems = subItemsMap.computeIfAbsent(joinField, s -> new ArrayList<>());

            int siRes = (int) buffer.getVarUInt();
            for (int i = 0; i < siRes; i++) {
                ItemParams subItemParams = readItemParams();
                int subItemLength = (int) buffer.getUInt32();
                Object subItem = subItemItemReader.readItem(new ByteBuffer(buffer.getBytes(subItemLength)).rewind());
                subItems.add(subItem);
            }
        }

        subItemsMap.forEach((key, value) -> writeJoinResult(item, key, value));

        position++;
        return item;

    }

    private void writeJoinResult(T item, String fieldName, List<Object> subItems) {
        Field field = FieldUtils.getField(item.getClass(), fieldName, true);

        if (field == null || !field.isAnnotationPresent(Transient.class)) {
            String msg = String.format("Join results omitted: no transient field '%s' found", fieldName);
            LOGGER.debug(msg);
        }

        if (field != null) {
            if (field.getType() == List.class) {
                BeanPropertyUtils.setProperty(item, fieldName, subItems);
            } else {
                if (subItems.size() > 1) {
                    throw new RuntimeException("Multiple join result found: " + fieldName);
                } else if (subItems.size() == 0) {
                    BeanPropertyUtils.setProperty(item, fieldName, null);
                } else {
                    BeanPropertyUtils.setProperty(item, fieldName, subItems.get(0));
                }
            }
        }
    }

    private int getJoinedNsIndexOffset(int nsId) {
        if (query == null) {
            return 1;
        }

        int offset = 1 + query.getMergeQueries().size();
        int mergedNsIdx = nsId;

        if (mergedNsIdx > 0) {
            offset += query.getJoinQueries().size();
            mergedNsIdx--;
        }

        for (int i = 0; i < mergedNsIdx; i++) {
            offset += query.getMergeQueries().get(i).getJoinQueries().size();
        }

        return offset;
    }

    private ItemParams readItemParams() {
        ItemParams params = new ItemParams();

        if (queryResult.isWithItemId()) {
            params.id = buffer.getVarUInt();
            params.version = buffer.getVarUInt();
        }

        if (queryResult.isWithNsId()) {
            params.nsId = (int) buffer.getVarUInt();
        }

        if (queryResult.isWithRank()) {
            //used for full-text search
            params.rank = buffer.getVarUInt();
        }

        return params;
    }

    private boolean needFetch() {
        return this.position == count;
    }

    private void fetchResults() {
        requestContext.fetchResults(position, fetchCount);
        queryResult = requestContext.getQueryResult();
        parseQueryResult(queryResult);
    }

    @Override
    public long size() {
        return queryResult.getQCount();
    }

    @Override
    public List<AggregationResult> aggResults() {
        return queryResult.getAggResults();
    }

    /**
     * Closes the request context.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        requestContext.closeResults();
        closed = true;
    }

    private static class ItemParams {
        private long rank = -1;
        private long id = -1;
        private long version = -1;
        private int nsId = -1;

        public long getRank() {
            return rank;
        }

        public long getId() {
            return id;
        }

        public long getVersion() {
            return version;
        }

        public int getNsId() {
            return nsId;
        }
    }

}
