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

import org.apache.commons.lang3.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.rt.restream.reindexer.annotations.Transient;
import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.binding.QueryResult;
import ru.rt.restream.reindexer.binding.RequestContext;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.binding.cproto.ItemReader;
import ru.rt.restream.reindexer.binding.cproto.cjson.CjsonItemReader;
import ru.rt.restream.reindexer.binding.cproto.cjson.CtagMatcher;
import ru.rt.restream.reindexer.binding.cproto.cjson.PayloadType;
import ru.rt.restream.reindexer.util.BeanPropertyUtils;
import ru.rt.restream.reindexer.util.NativeUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An iterator over a query result.
 * Maintains a cursor pointing to its current row of data. Initially the cursor is positioned before the first row.
 */
public class QueryResultIterator<T> implements ResultIterator<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryResultIterator.class);

    private final ReindexerNamespace<?> namespace;

    private final Class<T> itemClass;

    private final RequestContext requestContext;

    private final int fetchCount;

    private ItemReader<T> itemReader;

    private ByteBuffer buffer;

    private QueryResult queryResult;

    private Query<?> query;

    private int position;

    private long count;

    private boolean closed;

    private float currentRank;

    public QueryResultIterator(ReindexerNamespace<?> namespace,
                               Class<T> itemClass,
                               RequestContext requestContext,
                               Query<?> query,
                               int fetchCount) {
        this.namespace = namespace;
        this.itemClass = itemClass;
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
                itemReader = new CjsonItemReader<>(itemClass, ctagMatcher);
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

        T item = itemClass.cast(readItem(namespace, itemReader, query));
        position++;
        return item;

    }

    private <S> S readItem(ReindexerNamespace<?> expectedNamespace, ItemReader<S> reader, Query<?> queryContext) {
        ItemParams params = readItemParams();
        Query<?> itemQueryContext = getItemQueryContext(queryContext, params.nsId);

        ReindexerNamespace<?> itemNamespace = expectedNamespace;
        if (query != null && params.nsId < query.getNamespaces().size()) {
            itemNamespace = query.getNamespaces().get(params.nsId);
        }

        S item = readItemData(params, reader, itemNamespace);
        readJoinedItems(item, itemQueryContext, params.nsId);
        return item;
    }

    private Query<?> getItemQueryContext(Query<?> defaultQueryContext, int nsId) {
        if (query == null || nsId == 0) {
            return defaultQueryContext;
        }

        List<Query<?>> mergeQueries = query.getMergeQueries();
        if (nsId <= mergeQueries.size()) {
            return mergeQueries.get(nsId - 1);
        }

        return defaultQueryContext;
    }

    private <S> S readItemData(ItemParams params, ItemReader<S> reader, ReindexerNamespace<?> itemNamespace) {
        if (params.cptr != 0) {
            ByteBuffer nativeBuffer = NativeUtils.getNativeBuffer(queryResult.getResultsPtr(), params.cptr,
                    params.nsId);
            return reader.readItem(nativeBuffer);
        }

        int length = (int) buffer.getUInt32();
        return reader.readItem(new ByteBuffer(buffer.getBytes(length)).rewind());
    }

    private void readJoinedItems(Object item, Query<?> queryContext, int nsId) {
        if (!queryResult.isWithJoined()) {
            return;
        }

        if (queryResult.getQueryFormatVersion() == Consts.QUERY_FORMAT_V1) {
            readJoinedItemsV1(item, nsId);
            return;
        }

        int joinedFields = (int) buffer.getVarUInt();
        Map<String, List<Object>> subItemsMap = new HashMap<>();
        for (int joinedField = 0; joinedField < joinedFields; joinedField++) {
            int itemsCount = (int) buffer.getVarUInt();
            if (queryContext == null) {
                skipJoinedItems(itemsCount);
                continue;
            }

            Query<?> joinQuery = queryContext.getJoinQueries().get(joinedField);
            ReindexerNamespace<?> joinedNamespace = joinQuery.getNamespace();
            CjsonItemReader<?> joinedItemReader = newItemReader(joinedNamespace);
            List<Object> subItems = new ArrayList<>(itemsCount);
            for (int i = 0; i < itemsCount; i++) {
                subItems.add(readItem(joinedNamespace, joinedItemReader, joinQuery));
            }
            subItemsMap.computeIfAbsent(queryContext.getJoinFields().get(joinedField), field -> new ArrayList<>())
                    .addAll(subItems);
        }
        subItemsMap.forEach((key, value) -> writeJoinResult(item, key, value));
    }

    private void readJoinedItemsV1(Object item, int nsId) {
        int joinedFields = (int) buffer.getVarUInt();
        if (query == null) {
            for (int joinedField = 0; joinedField < joinedFields; joinedField++) {
                skipJoinedItemsV1((int) buffer.getVarUInt());
            }
            return;
        }

        int namespaceIndexOffset = getJoinedNsIndexOffset(nsId);
        for (int nsIndex = 0; nsIndex < joinedFields; nsIndex++) {
            int itemsCount = (int) buffer.getVarUInt();
            ReindexerNamespace<?> joinedNamespace = query.getNamespaces().get(nsIndex + namespaceIndexOffset);
            CjsonItemReader<?> joinedItemReader = newItemReader(joinedNamespace);
            List<Object> subItems = new ArrayList<>(itemsCount);
            for (int j = 0; j < itemsCount; j++) {
                ItemParams subItemParams = readItemParams();
                subItems.add(readItemData(subItemParams, joinedItemReader, joinedNamespace));
            }

            writeJoinResult(item, query.getJoinFields().get(nsIndex), subItems);
        }
    }

    private void skipJoinedItems(int itemsCount) {
        for (int i = 0; i < itemsCount; i++) {
            ItemParams itemParams = readItemParams();
            if (itemParams.cptr == 0) {
                int length = (int) buffer.getUInt32();
                buffer.skip(length);
            }
            readJoinedItems(null, null, itemParams.nsId);
        }
    }

    private void skipJoinedItemsV1(int itemsCount) {
        for (int i = 0; i < itemsCount; i++) {
            ItemParams itemParams = readItemParams();
            if (itemParams.cptr == 0) {
                int length = (int) buffer.getUInt32();
                buffer.skip(length);
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

    private CjsonItemReader<?> newItemReader(ReindexerNamespace<?> itemNamespace) {
        PayloadType payloadType = itemNamespace.getPayloadType();
        CtagMatcher ctagMatcher = new CtagMatcher();
        ctagMatcher.read(payloadType);
        return new CjsonItemReader<>(itemNamespace.getItemClass(), ctagMatcher);
    }

    private void writeJoinResult(Object item, String fieldName, List<Object> subItems) {
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
            if (queryResult.getRankFormat() == 0) {
                params.rank = buffer.getFloat();
                currentRank = params.rank;
            } else {
                params.rank = buffer.getVarUInt();
            }
        }

        if (queryResult.isWithShardId()) {
            if (queryResult.getShardId() != Consts.SHARDING_PROXY_OFF) {
                params.shardId = queryResult.getShardId();
            } else {
                params.shardId = (int) buffer.getVarUInt();
            }

        }

        if (queryResult.isWithResultsPtr()) {
            params.cptr = buffer.getUInt64();
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
    public long getTotalCount() {
        return queryResult.getTotalCount();
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
    public float getCurrentRank() {
        if (queryResult.isWithRank()) {
            return currentRank;
        }
        return Consts.EMPTY_RANK;
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
        private float rank = -1;
        private long id = -1;
        private int shardId = Consts.SHARDING_PROXY_OFF;
        private long version = -1;
        private long cptr;
        private int nsId = 0;

        public float getRank() {
            return rank;
        }

        public long getId() {
            return id;
        }

        public int getShardId() {
            return shardId;
        }

        public long getVersion() {
            return version;
        }

        public long getCptr() {
            return cptr;
        }

        public int getNsId() {
            return nsId;
        }
    }

}
