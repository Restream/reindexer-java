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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static ru.rt.restream.reindexer.QueryLogBuilder.AggregateType.AVG;
import static ru.rt.restream.reindexer.QueryLogBuilder.AggregateType.DISTINCT;
import static ru.rt.restream.reindexer.QueryLogBuilder.AggregateType.MAX;
import static ru.rt.restream.reindexer.QueryLogBuilder.AggregateType.MIN;
import static ru.rt.restream.reindexer.QueryLogBuilder.AggregateType.SUM;
import static ru.rt.restream.reindexer.QueryLogBuilder.QueryType.DELETE;
import static ru.rt.restream.reindexer.QueryLogBuilder.QueryType.SELECT;
import static ru.rt.restream.reindexer.QueryLogBuilder.QueryType.UPDATE;
import static ru.rt.restream.reindexer.binding.Consts.INNER_JOIN;
import static ru.rt.restream.reindexer.binding.Consts.LEFT_JOIN;
import static ru.rt.restream.reindexer.binding.Consts.MERGE;
import static ru.rt.restream.reindexer.binding.Consts.MODE_ACCURATE_TOTAL;
import static ru.rt.restream.reindexer.binding.Consts.OR_INNER_JOIN;
import static ru.rt.restream.reindexer.binding.Consts.VALUE_BOOL;
import static ru.rt.restream.reindexer.binding.Consts.VALUE_NULL;
import static ru.rt.restream.reindexer.binding.Consts.VALUE_STRING;

/**
 * An instance of this class provides methods for building a query for database objects.
 */
public class Query<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Query.class);
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
    private static final int QUERY_BETWEEN_FIELDS_CONDITION = 26;
    private static final int QUERY_ALWAYS_FALSE_CONDITION = 27;

    /**
     * Condition types.
     */
    public enum Condition {
        /**
         * Index contains any non-null value.
         */
        ANY(0),

        /**
         * Index contains a value equal to the passed to condition
         */
        EQ(1),

        /**
         * Index contains a value, that is lower than the passed to condition
         */
        LT(2),

        /**
         * Index contains a value, that is lower or equal to the value passed to condition
         */
        LE(3),

        /**
         * Index contains a value, that is greater than the passed to condition
         */
        GT(4),

        /**
         * Index contains a value, that is greater or equal to the value passed to condition
         */
        GE(5),

        /**
         * Index contains a value, that is between the values passed to condition
         */
        RANGE(6),

        /**
         * Index contains a value, that belongs to the set of values passed to condition
         */
        SET(7),

        /**
         * Index contains all values from the set of values passed to condition
         */
        ALLSET(8),

        /**
         * Index contains an empty value
         */
        EMPTY(9),

        /**
         * Index String like pattern
         */
        LIKE(10);

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

    private final QueryLogBuilder logBuilder = new QueryLogBuilder();

    private int queryCount = 0;

    private int joinType;

    private Query<?> root;

    Query(Reindexer reindexer, ReindexerNamespace<T> namespace, TransactionContext transactionContext) {
        logBuilder.namespace(namespace.getName());
        this.reindexer = reindexer;
        this.namespace = namespace;
        this.transactionContext = transactionContext;
        buffer.putVString(namespace.getName());
    }

    /**
     * Selects only {@code fields} of items, another fields will be omitted.
     *
     * @param fields fields to select
     * @return the {@link Query} for further customizations
     */
    public Query<T> select(String... fields) {
        for (String field : fields) {
            logBuilder.select(field);
            buffer.putVarUInt32(QUERY_SELECT_FILTER).putVString(field);
        }
        return this;
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
        logBuilder.join(joinQuery.logBuilder, joinType);
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

    /**
     * Specify the join condition.
     *
     * @param joinField the join field of the right side of the join
     * @param condition the joining condition. {@link Condition}
     * @param joinIndex the join index of the left side of join
     * @return the {@link Query} for further customizations
     */
    public Query<T> on(String joinField, Condition condition, String joinIndex) {
        logBuilder.on(nextOperation, joinField, condition.code, joinIndex);
        buffer.putVarUInt32(QUERY_JOIN_ON);
        buffer.putVarUInt32(nextOperation);
        buffer.putVarUInt32(condition.code);
        buffer.putVString(joinField);
        buffer.putVString(joinIndex);
        nextOperation = OP_AND;
        return this;
    }

    /**
     * Merge 2 queries of the same type.
     *
     * @param mergeQuery query to merge
     * @return the {@link Query} for further customizations
     */
    public Query<T> merge(Query<T> mergeQuery) {
        logBuilder.merge(mergeQuery.logBuilder);
        mergeQueries.add(mergeQuery);
        return this;
    }

    /**
     * The 'is NULL' condition is supported only by 'sparse' or 'array' indexes.
     *
     * @param indexName index name
     * @return the {@link Query} for further customizations
     */
    public Query<T> isNull(String indexName) {
        return where(indexName, Condition.EMPTY);
    }

    /**
     * The 'NOT NULL' condition is supported only by 'sparse' or 'array' indexes.
     *
     * @param indexName index name
     * @return the {@link Query} for further customizations
     */
    public Query<T> isNotNull(String indexName) {
        return where(indexName, Condition.ANY);
    }

    /**
     * Query with a String like pattern.
     *
     * @param indexName index name
     * @param pattern   String like pattern
     * @return the {@link Query} for further customizations
     */
    public Query<T> like(String indexName, String pattern) {
        return where(indexName, Condition.LIKE, pattern);
    }

    /**
     * Queries are possible only on the indexed fields, marked with reindex annotation.
     *
     * @param indexName index name
     * @param condition condition value {@link Condition}
     * @param values    values to match
     * @return the {@link Query} for further customizations
     */
    public Query<T> where(String indexName, Condition condition, Collection<?> values) {
        logBuilder.where(nextOperation, indexName, condition.code, values);
        buffer.putVarUInt32(QUERY_CONDITION)
                .putVString(indexName)
                .putVarUInt32(nextOperation)
                .putVarUInt32(condition.code);

        this.nextOperation = OP_AND;
        this.queryCount++;

        if (values != null) {
            buffer.putVarUInt32(values.size());
            for (Object key : values) {
                putValue(key);
            }
        }

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
        logBuilder.where(nextOperation, indexName, condition.code, values);
        buffer.putVarUInt32(QUERY_CONDITION)
                .putVString(indexName)
                .putVarUInt32(nextOperation)
                .putVarUInt32(condition.code);

        this.nextOperation = OP_AND;
        this.queryCount++;

        if (values != null) {
            buffer.putVarUInt32(values.length);
            for (Object key : values) {
                putValue(key);
            }
        }

        return this;
    }

    /**
     * Where - Add comparing two fields where condition to DB query.
     *
     * @param firstField  the first field to use
     * @param condition   the {@link Condition} to use
     * @param secondField the second field to use
     * @return the {@link Query} for further customizations
     */
    public Query<T> whereBetweenFields(String firstField, Condition condition, String secondField) {
        logBuilder.whereBetweenFields(nextOperation, firstField, condition.code, secondField);
        buffer.putVarUInt32(QUERY_BETWEEN_FIELDS_CONDITION)
                .putVarUInt32(nextOperation)
                .putVString(firstField)
                .putVarUInt32(condition.code)
                .putVString(secondField);
        nextOperation = OP_AND;
        queryCount++;
        return this;
    }

    /**
     * Open bracket for where condition to DB query
     *
     * @return the {@link Query} for further customizations
     */
    public Query<T> openBracket() {
        logBuilder.openBracket(nextOperation);
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
        logBuilder.closeBracket();
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
        logBuilder.aggregate(DISTINCT, field);
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
        logBuilder.aggregate(SUM, field);
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
        logBuilder.aggregate(AVG, field);
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
        logBuilder.aggregate(MIN, field);
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
        logBuilder.aggregate(MAX, field);
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
        AggregationFacetRequest facet = new AggregationFacetRequest();
        logBuilder.aggregate(facet, fields);
        buffer.putVarUInt32(QUERY_AGGREGATION).putVarUInt32(AGG_FACET).putVarUInt32(fields.length);
        for (String field : fields) {
            buffer.putVString(field);
        }
        return facet;
    }

    /**
     * Builder for a facet aggregation.
     */
    public class AggregationFacetRequest {

        /**
         * Limit facet aggregation results.
         *
         * @param limit facet aggregation limit
         * @return the {@link AggregationFacetRequest} for further customizations
         */
        public AggregationFacetRequest limit(int limit) {
            logBuilder.facetLimit(this, limit);
            buffer.putVarUInt32(QUERY_AGGREGATION_LIMIT).putVarUInt32(limit);
            return this;
        }

        /**
         * Set offset of the facet aggregation results.
         *
         * @param offset facet aggregation offset
         * @return the {@link AggregationFacetRequest} for further customizations
         */
        public AggregationFacetRequest offset(int offset) {
            logBuilder.facetOffset(this, offset);
            buffer.putVarUInt32(QUERY_AGGREGATION_OFFSET).putVarUInt32(offset);
            return this;
        }

        /**
         * Sort facets by field value.
         *
         * @param field item field. Use field 'count' to sort by facet's count value
         * @param desc  true if descending order
         * @return the {@link AggregationFacetRequest} for further customizations
         */
        public AggregationFacetRequest sort(String field, boolean desc) {
            logBuilder.facetSort(this, field, desc);
            buffer.putVarUInt32(QUERY_AGGREGATION_SORT).putVString(field);
            if (desc) {
                buffer.putVarUInt32(1);
            } else {
                buffer.putVarUInt32(0);
            }
            return this;
        }

    }


    /**
     * Calculate the total count of matching documents.
     *
     * @return the {@link Query} for further customizations
     */
    public Query<T> reqTotal() {
        logBuilder.reqTotal();
        buffer.putVarUInt32(QUERY_REQ_TOTAL)
                .putVarUInt32(MODE_ACCURATE_TOTAL);
        return this;
    }

    /**
     * Limit is used to retrieve records from the namespace in a database and limit the number of items returned based
     * on a limit value
     *
     * @param limit limit value
     * @return the {@link Query} for further customizations
     */
    public Query<T> limit(int limit) {
        if (limit >= 0) {
            logBuilder.limit(limit);
            buffer.putVarUInt32(QUERY_LIMIT)
                    .putVarUInt32(limit);
        }
        return this;
    }

    /**
     * Skip the offset items before start.
     *
     * @param offset offset value
     * @return the {@link Query} for further customizations
     */
    public Query<T> offset(int offset) {
        if (offset > 0) {
            logBuilder.offset(offset);
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
        logBuilder.sort(index, desc, values);
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
        } else if (value instanceof UUID) {
            buffer.putVarUInt32(Consts.VALUE_UUID)
                    .putUuid((UUID) value);
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
        ResultIterator<T> iterator = execute();
        Spliterator<T> spliterator = Spliterators.spliterator(iterator, iterator.size(), Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, false).onClose(iterator::close);
    }

    /**
     * Will execute query, and return list of items.
     *
     * @return list of items
     */
    public List<T> toList() {
        try (ResultIterator<T> iterator = execute()) {
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
        try (ResultIterator<T> iterator = execute()) {
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
        try (ResultIterator<T> iterator = execute()) {
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
        try (ResultIterator<T> iterator = execute()) {
            return iterator.hasNext();
        }
    }

    /**
     * Will execute query, and return slice of items.
     *
     * @return an iterator over a query result
     */
    public ResultIterator<T> execute() {
        return execute(namespace.getItemClass());
    }

    /**
     * Will execute query, and return slice of items.
     *
     * @param <S>       the item type
     * @param itemClass the item class
     * @return an iterator over a query result
     */
    public <S> ResultIterator<S> execute(Class<S> itemClass) {
        long[] ptVersions = prepareQueryAndGetPayloadTypesVersions();

        RequestContext requestContext = transactionContext != null
                ? transactionContext.selectQuery(buffer.bytes(), fetchCount, ptVersions, false)
                : reindexer.getBinding().selectQuery(buffer.bytes(), fetchCount, ptVersions, false);

        updatePayloadTypes(requestContext.getQueryResult());

        return new QueryResultIterator<>(namespace, itemClass, requestContext, this, fetchCount);
    }

    /**
     * Will execute query, and return slice of items.
     *
     * @return an iterator over a query result
     */
    public QueryResultJsonIterator executeToJson() {
        long[] ptVersions = prepareQueryAndGetPayloadTypesVersions();

        RequestContext requestContext = transactionContext != null
                ? transactionContext.selectQuery(buffer.bytes(), fetchCount, ptVersions, true)
                : reindexer.getBinding().selectQuery(buffer.bytes(), fetchCount, ptVersions, true);

        QueryResult queryResult = requestContext.getQueryResult();

        updatePayloadTypes(queryResult);

        return new QueryResultJsonIterator(requestContext, fetchCount);
    }

    private void updatePayloadTypes(QueryResult queryResult) {
        for (PayloadType payloadType : queryResult.getPayloadTypes()) {
            ReindexerNamespace<?> namespace = namespaces.get((int) payloadType.getNamespaceId());
            PayloadType currentPayloadType = namespace.getPayloadType();
            if (currentPayloadType == null || currentPayloadType.getVersion() < payloadType.getVersion()
                || currentPayloadType.getStateToken() != payloadType.getStateToken()) {
                namespace.updatePayloadType(payloadType);
            }
        }
    }

    private long[] prepareQueryAndGetPayloadTypesVersions() {
        logBuilder.type(SELECT);
        if (LOGGER.isDebugEnabled()) {
            debug();
            LOGGER.debug(logBuilder.getSql());
        }

        namespaces.add(namespace);

        for (Query<?> mergeQuery : mergeQueries) {
            namespaces.add(mergeQuery.namespace);
        }

        for (Query<?> joinQuery : joinQueries) {
            namespaces.add(joinQuery.namespace);
        }

        for (Query<?> mergeQuery : mergeQueries) {
            for (Query<?> joinQuery : mergeQuery.joinQueries) {
                namespaces.add(joinQuery.namespace);
            }
        }

        buffer.putVarUInt32(QUERY_END);

        for (Query<?> joinQuery : joinQueries) {
            buffer.putVarUInt32(joinQuery.joinType);
            buffer.writeBytes(joinQuery.buffer.bytes());
            buffer.putVarUInt32(QUERY_END);
        }

        for (Query<?> mergeQuery : mergeQueries) {
            buffer.putVarUInt32(MERGE);
            buffer.writeBytes(mergeQuery.buffer.bytes());
            buffer.putVarUInt32(QUERY_END);
            List<Query<?>> joinQueries = mergeQuery.getJoinQueries();
            for (Query<?> joinQuery : joinQueries) {
                buffer.putVarUInt32(joinQuery.joinType);
                buffer.writeBytes(joinQuery.buffer.bytes());
                buffer.putVarUInt32(QUERY_END);
            }
        }

        return namespaces.stream()
                .map(ReindexerNamespace::getPayloadType)
                .map(pt -> pt == null ? 0 : pt.getStateToken())
                .mapToLong(Integer::longValue)
                .toArray();
    }

    /**
     * Will execute query, and delete items, matches query.
     */
    public void delete() {
        logBuilder.type(DELETE);
        if (LOGGER.isDebugEnabled()) {
            debug();
            LOGGER.debug(logBuilder.getSql());
        }
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
        logBuilder.set(fieldName, value);
        int cmd = QUERY_UPDATE_FIELD;
        if (value instanceof Collection<?>) { //Not tested
            Collection<?> values = (Collection<?>) value;
            if (values.size() <= 1) {
                cmd = QUERY_UPDATE_FIELD_V2;
            }
            buffer.putVarUInt32(cmd);
            buffer.putVString(fieldName);
            if (cmd == QUERY_UPDATE_FIELD_V2) {
                buffer.putVarUInt32(1);
            }
            buffer.putVarUInt32(values.size());
            for (Object v : values) {
                buffer.putVarUInt32(0);
                putValue(v);
            }
        } else if (value != null && value.getClass().isArray()) {
            Object[] values = (Object[]) value;
            if (values.length <= 1) {
                cmd = QUERY_UPDATE_FIELD_V2;
            }
            buffer.putVarUInt32(cmd);
            buffer.putVString(fieldName);
            if (cmd == QUERY_UPDATE_FIELD_V2) {
                buffer.putVarUInt32(1);
            }
            buffer.putVarUInt32(values.length);
            for (Object v : values) {
                buffer.putVarUInt32(0);
                putValue(v);
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
        if (isArray) {
            logBuilder.setObject(fieldName, jsons);
        } else {
            logBuilder.setObject(fieldName, jsons.get(0));
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
        logBuilder.drop(field);
        buffer.putVarUInt32(QUERY_DROP_FIELD);
        buffer.putVString(field);
        return this;
    }

    private Query<T> debug() {
        buffer.putVarUInt32(QUERY_DEBUG_LEVEL).putVarUInt32(4); //4 - TRACE
        return this;
    }

    /**
     * Will execute query, and update fields in items, which matches query.
     */
    public void update() {
        logBuilder.type(UPDATE);
        if (LOGGER.isDebugEnabled()) {
            debug();
            LOGGER.debug(logBuilder.getSql());
        }
        if (transactionContext != null) {
            transactionContext.updateQuery(buffer.bytes());
        } else {
            reindexer.getBinding().updateQuery(buffer.bytes());
        }
    }

    /**
     * Return joined queries.
     */
    public List<Query<?>> getJoinQueries() {
        return joinQueries;
    }

    /**
     * Return merged queries.
     */
    public List<Query<?>> getMergeQueries() {
        return mergeQueries;
    }

    /**
     * Get the namespace names that are used in the current query.
     */
    public List<ReindexerNamespace<?>> getNamespaces() {
        return namespaces;
    }

    /**
     * Get the joined fields of the query.
     */
    public List<String> getJoinFields() {
        return joinFields;
    }

}
