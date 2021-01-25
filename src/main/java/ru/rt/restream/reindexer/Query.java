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

import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.binding.QueryResult;
import ru.rt.restream.reindexer.binding.RequestContext;
import ru.rt.restream.reindexer.binding.TransactionContext;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.binding.cproto.cjson.PayloadType;
import ru.rt.restream.reindexer.util.JsonSerializer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static ru.rt.restream.reindexer.binding.Consts.INNER_JOIN;
import static ru.rt.restream.reindexer.binding.Consts.LEFT_JOIN;
import static ru.rt.restream.reindexer.binding.Consts.OR_INNER_JOIN;
import static ru.rt.restream.reindexer.binding.Consts.VALUE_BOOL;
import static ru.rt.restream.reindexer.binding.Consts.VALUE_NULL;
import static ru.rt.restream.reindexer.binding.Consts.VALUE_STRING;

public class Query<T> {

    private static final int DEFAULT_FETCH_COUNT = 100;

    private static final int OP_OR = 1;
    private static final int OP_AND = 2;
    private static final int OP_NOT = 3;

    private static final int AGG_SUM = 0;
    private static final int AGG_AVG = 1;
    private static final int AGG_FACET = 2;
    private static final int AGG_MIN = 3;
    private static final int AGG_MAX = 4;
    private static final int AGG_DISTINCT = 5;

    private static final int QUERY_CONDITION = 0;
    private static final int QUERY_DISTINCT = 1;
    private static final int QUERY_SORT_INDEX = 2;
    private static final int QUERY_JOIN_ON = 3;
    private static final int QUERY_LIMIT = 4;
    private static final int QUERY_OFFSET = 5;
    private static final int QUERY_REQ_TOTAL = 6;
    private static final int QUERY_DEBUG_LEVEL = 7;
    private static final int QUERY_AGGREGATION = 8;
    private static final int QUERY_SELECT_FILTER = 9;
    private static final int QUERY_SELECT_FUNCTION = 10;
    private static final int QUERY_END = 11;
    private static final int QUERY_EXPLAIN = 12;
    private static final int QUERY_EQUAL_POSITION = 13;
    private static final int QUERY_UPDATE_FIELD = 14;
    private static final int QUERY_AGGREGATION_LIMIT = 15;
    private static final int QUERY_AGGREGATION_OFFSET = 16;
    private static final int QUERY_AGGREGATION_SORT = 17;
    private static final int QUERY_OPEN_BRACKET = 18;
    private static final int QUERY_CLOSE_BRACKET = 19;
    private static final int QUERY_JOIN_CONDITION = 20;
    private static final int QUERY_DROP_FIELD = 21;
    private static final int QUERY_UPDATE_OBJECT = 22;
    private static final int QUERY_UPDATE_FIELD_V2 = 25;

    public enum Condition {
        ANY(0),
        EQ(1),
        LT(2),
        LE(3),
        GT(4),
        GE(5),
        RANGE(6),
        SET(7),
        ALLSET(8),
        EMPTY(9);
        private final int code;

        Condition(int code) {
            this.code = code;
        }
    }

    private final Reindexer reindexer;

    private final ByteBuffer buffer = new ByteBuffer();

    private int nextOperation = OP_AND;

    private final ReindexerNamespace<T> namespace;

    private final TransactionContext transactionContext;

    private int fetchCount = DEFAULT_FETCH_COUNT;

    private final List<Query<?>> joinQueries = new ArrayList<>();

    private final List<String> joinFields = new ArrayList<>();

    private final List<Query<?>> mergeQueries = new ArrayList<>();

    private final List<ReindexerNamespace<?>> namespaces = new ArrayList<>();

    private Deque<Integer> openedBrackets = new ArrayDeque<>();

    private int queryCount = 0;

    private int joinType;

    private Query<?> root;

    public Query(Reindexer reindexer, ReindexerNamespace<T> namespace, TransactionContext transactionContext) {
        this.reindexer = reindexer;
        this.namespace = namespace;
        this.transactionContext = transactionContext;
        buffer.putVString(namespace.getName());
    }

    /**
     * Inner joins 2 queries, alias for innerJoin.
     *
     * @param <J>       type of joined items
     * @param joinQuery query to join
     * @param field     parameter serves as unique identifier for the join between queries. If left side namespace has
     *                  {@link ru.rt.restream.reindexer.annotations.Transient} field with the same name, that field will
     *                  be populated with join results
     * @return this {@link Query} for further customizations
     */
    public <J> Query<T> join(Query<J> joinQuery, String field) {
        return innerJoin(joinQuery, field);
    }

    /**
     * Inner joins 2 queries.
     *
     * @param <J>       type of joined items
     * @param joinQuery query to join
     * @param field     parameter serves as unique identifier for the join between queries. If left side namespace has
     *                  {@link ru.rt.restream.reindexer.annotations.Transient} field with the same name, that field will
     *                  be populated with join results
     * @return this {@link Query} for further customizations
     */
    public <J> Query<T> innerJoin(Query<J> joinQuery, String field) {
        if (nextOperation == OP_OR) {
            nextOperation = OP_AND;
            return join(joinQuery, field, OR_INNER_JOIN);
        }

        return join(joinQuery, field, INNER_JOIN);
    }

    /**
     * Left joins 2 queries.
     *
     * @param <J>       type of joined items
     * @param joinQuery query to join
     * @param field     parameter serves as unique identifier for the join between queries. If left side namespace has
     *                  {@link ru.rt.restream.reindexer.annotations.Transient} field with the same name, that field will
     *                  be populated with join results
     * @return this {@link Query} for further customizations
     */
    public <J> Query<T> leftJoin(Query<J> joinQuery, String field) {
        return join(joinQuery, field, LEFT_JOIN);
    }

    private <J> Query<T> join(Query<J> joinQuery, String field, int joinType) {
        if (joinQuery.root != null) {
            throw new IllegalStateException("query.join call on already joined query. You should create new Query");
        }

        if (joinType != LEFT_JOIN) {
            buffer.putVarUInt32(QUERY_JOIN_CONDITION);
            buffer.putVarUInt32(joinType);
            buffer.putVarUInt32(joinQueries.size()); // index of join query
        }

        joinQuery.joinType = joinType;
        joinQuery.root = this;
        joinQueries.add(joinQuery);
        joinFields.add(field);
        return this;
    }

    public Query<T> on(String joinField, Condition condition, String joinIndex) {
        buffer.putVarUInt32(QUERY_JOIN_ON);
        buffer.putVarUInt32(nextOperation);
        buffer.putVarUInt32(condition.code);
        buffer.putVString(joinField);
        buffer.putVString(joinIndex);
        nextOperation = OP_AND;
        return this;
    }

    /**
     * Queries are possible only on the indexed fields, marked with reindex annotation.
     *
     * @param indexName index name
     * @param condition condition value {@link Condition}
     * @param values    values to match
     * @return the {@link Query} for further customizations
     */
    public Query<T> where(String indexName, Condition condition, Object... values) {
        buffer.putVarUInt32(QUERY_CONDITION)
                .putVString(indexName)
                .putVarUInt32(nextOperation)
                .putVarUInt32(condition.code);

        this.nextOperation = OP_AND;
        this.queryCount++;

        if (values != null && values.length > 0) {
            buffer.putVarUInt32(values.length);
            for (Object key : values) {
                putValue(key);
            }
        }

        return this;
    }

    /**
     * Open bracket for where condition to DB query
     *
     * @return the {@link Query} for further customizations
     */
    public Query<T> openBracket() {
        buffer.putVarUInt32(QUERY_OPEN_BRACKET);
        buffer.putVarUInt32(nextOperation);
        nextOperation = OP_AND;
        this.openedBrackets.add(this.queryCount++);
        return this;
    }

    /**
     * Close bracket for where condition to DB query
     *
     * @return the {@link Query} for further customizations
     */
    public Query<T> closeBracket() {
        if (nextOperation != OP_AND) {
            throw new RuntimeException("Operation before close bracket");
        }
        if (openedBrackets.size() < 1) {
            throw new RuntimeException("Close bracket before open it");
        }
        buffer.putVarUInt32(QUERY_CLOSE_BRACKET);
        openedBrackets.pollLast();
        return this;
    }

    /**
     * Next condition will added with OR
     *
     * @return the {@link Query} for further customizations
     */
    public Query<T> or() {
        this.nextOperation = OP_OR;
        return this;
    }

    /**
     * Next condition will added with NOT
     *
     * @return the {@link Query} for further customizations
     */
    public Query<T> not() {
        this.nextOperation = OP_NOT;
        return this;
    }

    /**
     * Add where condition to DB query with interface args for composite indexes.
     *
     * @param indexName composite index name
     * @param condition condition value {@link Condition}
     * @param values    values of composite index to match
     * @return the {@link Query} for further customizations
     */
    public Query<T> whereComposite(String indexName, Condition condition, Object... values) {
        where(indexName, condition, new Object[]{values});
        return this;
    }

    /**
     * Get list of unique values of the field.
     *
     * @param field item field
     * @return the {@link Query} for further customizations
     */
    public Query<T> aggregateDistinct(String field) {
        buffer.putVarUInt32(QUERY_AGGREGATION).putVarUInt32(AGG_DISTINCT).putVarUInt32(1).putVString(field);
        return this;
    }

    /**
     * Get sum field value.
     *
     * @param field item field
     * @return the {@link Query} for further customizations
     */
    public Query<T> aggregateSum(String field) {
        buffer.putVarUInt32(QUERY_AGGREGATION).putVarUInt32(AGG_SUM).putVarUInt32(1).putVString(field);
        return this;
    }

    /**
     * Get average field value.
     *
     * @param field item field
     * @return the {@link Query} for further customizations
     */
    public Query<T> aggregateAvg(String field) {
        buffer.putVarUInt32(QUERY_AGGREGATION).putVarUInt32(AGG_AVG).putVarUInt32(1).putVString(field);
        return this;
    }

    /**
     * Get minimum field value.
     *
     * @param field item field
     * @return the {@link Query} for further customizations
     */
    public Query<T> aggregateMin(String field) {
        buffer.putVarUInt32(QUERY_AGGREGATION).putVarUInt32(AGG_MIN).putVarUInt32(1).putVString(field);
        return this;
    }

    /**
     * Get maximum field value.
     *
     * @param field item field
     * @return the {@link Query} for further customizations
     */
    public Query<T> aggregateMax(String field) {
        buffer.putVarUInt32(QUERY_AGGREGATION).putVarUInt32(AGG_MAX).putVarUInt32(1).putVString(field);
        return this;
    }

    /**
     * Get fields facet value. Applicable to multiple data fields and the result of that could be sorted by any data
     * column or 'count' and cut off by offset and limit. In order to support this functionality this method
     * returns AggregationFacetRequest which has methods sort, limit and offset.
     *
     * @param fields any data column name or 'count', fields should not be empty
     * @return the {@link AggregationFacetRequest} for further customizations
     */
    public AggregationFacetRequest aggregateFacet(String... fields) {
        buffer.putVarUInt32(QUERY_AGGREGATION).putVarUInt32(AGG_FACET).putVarUInt32(fields.length);
        for (String field : fields) {
            buffer.putVString(field);
        }

        return new AggregationFacetRequest(this);
    }

    public static class AggregationFacetRequest {

        private final Query<?> query;

        public AggregationFacetRequest(Query<?> query) {
            this.query = query;
        }

        public AggregationFacetRequest limit(int limit) {
            query.buffer.putVarUInt32(QUERY_AGGREGATION_LIMIT).putVarUInt32(limit);
            return this;
        }

        public AggregationFacetRequest offset(int offset) {
            query.buffer.putVarUInt32(QUERY_AGGREGATION_OFFSET).putVarUInt32(offset);
            return this;
        }

        /**
         * Sort facets by field value.
         *
         * @param field item field. Use field 'count' to sort by facet's count value
         * @param desc  true if descending order
         */
        public AggregationFacetRequest sort(String field, boolean desc) {
            query.buffer.putVarUInt32(QUERY_AGGREGATION_SORT).putVString(field);
            if (desc) {
                query.buffer.putVarUInt32(1);
            } else {
                query.buffer.putVarUInt32(0);
            }
            return this;
        }

    }

    public Query<T> limit(int limit) {
        if (limit >= 0) {
            buffer.putVarUInt32(QUERY_LIMIT)
                    .putVarUInt32(limit);
        }
        return this;
    }

    public Query<T> offset(int offset) {
        if (offset > 0) {
            buffer.putVarUInt32(QUERY_OFFSET)
                    .putVarUInt32(offset);
        }
        return this;
    }

    /**
     * Apply sort order to returned from query items. If values argument specified, then items equal to values, if found
     * will be placed in the top positions. For composite indexes values must be []interface{}, with value of each
     * subindex
     *
     * @param index  the index name
     * @param desc   true if sorting in descending order
     * @param values values to match
     * @return the {@link Query} for further customizations
     */
    public Query<T> sort(String index, boolean desc, Object... values) {

        buffer.putVarUInt32(QUERY_SORT_INDEX)
                .putVString(index);
        if (desc) {
            buffer.putVarUInt32(1);
        } else {
            buffer.putVarUInt32(0);
        }

        buffer.putVarUInt32(values.length);
        for (Object value : values) {
            putValue(value);
        }

        return this;
    }

    /**
     * FetchCount sets the number of items that will be fetched by one operation
     * When fetchCount {@literal <=} 0 query will fetch all results in one operation
     *
     * @param fetchCount items count to fetch
     * @return the {@link Query} for further customizations
     */
    public Query<T> fetchCount(int fetchCount) {
        this.fetchCount = fetchCount;
        return this;
    }

    private void putValue(Object value) {
        if (value == null) {
            buffer.putVarUInt32(VALUE_NULL);
        } else if (value instanceof Boolean) {
            buffer.putVarUInt32(VALUE_BOOL);
            if ((Boolean) value) {
                buffer.putVarUInt32(1);
            } else {
                buffer.putVarUInt32(0);
            }
        } else if (value instanceof Integer) {
            buffer.putVarUInt32(Consts.VALUE_INT)
                    .putVarInt64((Integer) value);
        } else if (value instanceof String) {
            buffer.putVarUInt32(Consts.VALUE_STRING)
                    .putVString((String) value);
        } else if (value instanceof Long) {
            buffer.putVarUInt32(Consts.VALUE_INT_64)
                    .putVarInt64((Long) value);
        } else if (value instanceof Byte) {
            buffer.putVarUInt32(Consts.VALUE_INT)
                    .putVarInt64((Byte) value);
        } else if (value instanceof Short) {
            buffer.putVarUInt32(Consts.VALUE_INT)
                    .putVarInt64((Short) value);
        } else if (value instanceof Double) {
            buffer.putVarUInt32(Consts.VALUE_DOUBLE)
                    .putDouble((Double) value);
        } else if (value instanceof Float) {
            Float floatValue = (Float) value;
            buffer.putVarUInt32(Consts.VALUE_DOUBLE)
                    .putDouble(floatValue.doubleValue());
        } else if (value instanceof Character) {
            Character character = (Character) value;
            buffer.putVarUInt32(Consts.VALUE_STRING)
                    .putVString(character.toString());
        } else if (value instanceof Object[]) {
            buffer.putVarUInt32(Consts.VALUE_TUPLE);
            Object[] objects = (Object[]) value;
            buffer.putVarUInt32(objects.length);
            for (Object object : objects) {
                putValue(object);
            }
        }
    }

    /**
     * Will execute query, and return stream of items.
     * The returned stream must be closed using the {@link Stream#close()} method or
     * by using a Java 7 try-with-resources block.
     *
     * @return stream of items
     */
    public Stream<T> stream() {
        CloseableIterator<T> iterator = execute();
        Spliterator<T> spliterator = Spliterators.spliterator(iterator, iterator.size(), Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, false).onClose(iterator::close);
    }

    /**
     * Will execute query, and return list of items.
     *
     * @return list of items
     */
    public List<T> toList() {
        try (CloseableIterator<T> iterator = execute()) {
            List<T> result = new ArrayList<>();
            while (iterator.hasNext()) {
                result.add(iterator.next());
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Will execute query, and return one item.
     *
     * @return one item
     * @throws IllegalStateException if there are more than one or zero items
     */
    public T getOne() {
        T item = getOneInternal();
        if (item == null) {
            throw new IllegalStateException("Exactly one item expected, but there is zero");
        }
        return item;
    }

    /**
     * Will execute query, and return Optional item.
     *
     * @return Optional item
     * @throws IllegalStateException if there are more than one item
     */
    public Optional<T> findOne() {
        T item = getOneInternal();
        return Optional.ofNullable(item);
    }

    private T getOneInternal() {
        try (CloseableIterator<T> iterator = execute()) {
            T item = null;
            if (iterator.hasNext()) {
                item = iterator.next();
            }
            if (iterator.hasNext()) {
                throw new IllegalStateException("Exactly one item expected, but there are more");
            }
            return item;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * Will execute query, and return count of items.
     *
     * @return count of items
     */
    public long count() {
        try (CloseableIterator<T> iterator = execute()) {
            return iterator.size();
        }
    }

    /**
     * Will execute query, and return true if the item does not exists.
     *
     * @return true if the item does not exists
     */
    public boolean notExists() {
        return !exists();
    }

    /**
     * Will execute query, and return true if the item exists.
     *
     * @return true if the item exists
     */
    public boolean exists() {
        try (CloseableIterator<T> iterator = execute()) {
            return iterator.hasNext();
        }
    }

    /**
     * Will execute query, and return slice of items.
     *
     * @return an iterator over a query result
     */
    public CloseableIterator<T> execute() {
        buffer.putVarUInt32(QUERY_END);

        namespaces.add(namespace);

        for (Query<?> mergeQuery : mergeQueries) {
            namespaces.add(mergeQuery.namespace);
        }

        for (Query<?> joinQuery : joinQueries) {
            buffer.putVarUInt32(joinQuery.joinType);
            buffer.writeBytes(joinQuery.buffer.bytes());
            buffer.putVarUInt32(QUERY_END);
            namespaces.add(joinQuery.namespace);
        }

        for (Query<?> mergeQuery : mergeQueries) {
            List<Query<?>> joinQueries = mergeQuery.getJoinQueries();
            for (Query<?> joinQuery : joinQueries) {
                namespaces.add(joinQuery.namespace);
            }
        }

        long[] ptVersions = namespaces.stream()
                .map(ReindexerNamespace::getPayloadType)
                .map(pt -> pt == null ? 0 : pt.getStateToken())
                .mapToLong(Integer::longValue)
                .toArray();
        RequestContext requestContext = transactionContext != null
                ? transactionContext.selectQuery(buffer.bytes(), fetchCount, ptVersions)
                : reindexer.getBinding().selectQuery(buffer.bytes(), fetchCount, ptVersions);

        QueryResult queryResult = requestContext.getQueryResult();
        for (PayloadType payloadType : queryResult.getPayloadTypes()) {
            ReindexerNamespace<?> namespace = namespaces.get((int) payloadType.getNamespaceId());
            PayloadType currentPayloadType = namespace.getPayloadType();
            if (currentPayloadType == null || currentPayloadType.getVersion() < payloadType.getVersion()) {
                namespace.updatePayloadType(payloadType);
            }
        }

        return new QueryResultIterator<>(namespace, requestContext, this, fetchCount);
    }

    /**
     * Will execute query, and delete items, matches query.
     */
    public void delete() {
        if (transactionContext != null) {
            transactionContext.deleteQuery(buffer.bytes());
        } else {
            reindexer.getBinding().deleteQuery(buffer.bytes());
        }
    }

    /**
     * Adds update field request for update query
     *
     * @param fieldName field name
     * @param value     updated value
     * @return the {@link Query} for further customizations
     */
    public Query<T> set(String fieldName, Object value) {
        if (!isPrimitive(value)) {
            setObject(fieldName, value);
            return this;
        }
        int cmd = QUERY_UPDATE_FIELD;
        if (value instanceof Collection<?>) { //Not tested
            Collection<?> values = (Collection<?>) value;
            if (values.size() <= 1) {
                cmd = QUERY_UPDATE_FIELD_V2;
            }
            buffer.putVarUInt32(cmd);
            buffer.putVString(fieldName);
            if (values.size() == 0) {
                buffer.putVarUInt32(0);
                buffer.putVarUInt32(0);
            } else {
                if (cmd == QUERY_UPDATE_FIELD_V2) {
                    buffer.putVarUInt32(1);
                }
                buffer.putVarUInt32(values.size());
                for (Object v : values) {
                    buffer.putVarUInt32(0);
                    putValue(v);
                }
            }
        } else if (value != null && value.getClass().isArray()) {
            Object[] values = (Object[]) value;
            if (values.length <= 1) {
                cmd = QUERY_UPDATE_FIELD_V2;
            }
            buffer.putVarUInt32(cmd);
            buffer.putVString(fieldName);
            if (values.length == 0) {
                buffer.putVarUInt32(0);
                buffer.putVarUInt32(0);
            } else {
                if (cmd == QUERY_UPDATE_FIELD_V2) {
                    buffer.putVarUInt32(1);
                }
                buffer.putVarUInt32(values.length);
                for (Object v : values) {
                    buffer.putVarUInt32(0);
                    putValue(v);
                }
            }
        } else {
            buffer.putVarUInt32(cmd);
            buffer.putVString(fieldName);
            buffer.putVarUInt32(1);
            buffer.putVarUInt32(0);
            putValue(value);
        }

        return this;
    }

    private void setObject(String fieldName, Object value) {
        boolean isArray = false;
        int count = 1;
        List<String> jsons = new ArrayList<>();
        if (value.getClass().isArray()) {
            isArray = true;
            Object[] array = (Object[]) value;
            count = array.length;
            for (Object element : array) {
                String json = JsonSerializer.toJson(element);
                jsons.add(json);
            }
        } else if (value instanceof Collection<?>) {
            isArray = true;
            Collection<?> collection = (Collection<?>) value;
            count = collection.size();
            for (Object element : collection) {
                String json = JsonSerializer.toJson(element);
                jsons.add(json);
            }
        } else {
            String json = JsonSerializer.toJson(value);
            jsons.add(json);
        }

        buffer.putVarUInt32(QUERY_UPDATE_OBJECT);
        buffer.putVString(fieldName);
        buffer.putVarUInt32(count);
        buffer.putVarUInt32(isArray ? 1 : 0);
        for (String json : jsons) {
            buffer.putVarUInt32(0);
            buffer.putVarUInt32(VALUE_STRING);
            buffer.putVString(json);
        }
    }

    private boolean isPrimitive(Object value) {
        if (value instanceof Collection<?>) {
            Collection<?> collection = (Collection<?>) value;
            if (collection.isEmpty()) {
                return true;
            } else {
                Object element = collection.iterator().next();
                return isPrimitive(element);
            }
        } else if (value != null && value.getClass().isArray()) {
            Object[] values = (Object[]) value;
            if (values.length == 0) {
                return true;
            } else {
                Object element = values[0];
                return isPrimitive(element);
            }
        } else {
            return value == null
                   || value instanceof Boolean
                   || value instanceof Number
                   || value instanceof String;
        }
    }

    /**
     * Drop removes field from item within Update statement.
     *
     * @param field field to drop
     * @return the {@link Query} for further customizations
     */
    public Query<T> drop(String field) {
        buffer.putVarUInt32(QUERY_DROP_FIELD);
        buffer.putVString(field);
        return this;
    }

    /**
     * Will execute query, and update fields in items, which matches query.
     */
    public void update() {
        if (transactionContext != null) {
            transactionContext.updateQuery(buffer.bytes());
        } else {
            reindexer.getBinding().updateQuery(buffer.bytes());
        }
    }

    public List<Query<?>> getJoinQueries() {
        return joinQueries;
    }

    public List<Query<?>> getMergeQueries() {
        return mergeQueries;
    }

    public List<ReindexerNamespace<?>> getNamespaces() {
        return namespaces;
    }

    public List<String> getJoinFields() {
        return joinFields;
    }

}
