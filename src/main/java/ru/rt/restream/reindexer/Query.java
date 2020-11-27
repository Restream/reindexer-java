package ru.rt.restream.reindexer;

import ru.rt.restream.reindexer.binding.Binding;
import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.binding.QueryResult;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.binding.cproto.CprotoIterator;

import java.util.Collection;

import static ru.rt.restream.reindexer.binding.Consts.*;

public class Query<T> {

    private static final int DEFAULT_FETCH_COUNT = 100;

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

    private final Binding binding;

    private final ByteBuffer buffer = new ByteBuffer();

    private int nextOperation = OP_AND;

    private final Namespace<T> namespace;

    private int fetchCount = DEFAULT_FETCH_COUNT;

    public Query(Binding binding, Namespace<T> namespace) {
        this.binding = binding;
        this.namespace = namespace;
        buffer.putVString(namespace.getName());
    }

    public Query<T> join(String joined) {
        return null;
    }

    /**
     * Queries are possible only on the indexed fields, marked with reindex annotation.
     *
     * @param indexName index name
     * @param condition condition value {@link Condition}
     * @param values    values to match
     */
    public Query<T> where(String indexName, Condition condition, Object... values) {
        buffer.putVarUInt32(Consts.QUERY_CONDITION)
                .putVString(indexName)
                .putVarUInt32(nextOperation)
                .putVarUInt32(condition.code);

        this.nextOperation = OP_AND;

        if (values != null && values.length > 0) {
            buffer.putVarUInt32(values.length);
            for (Object key : values) {
                putValue(key);
            }
        }

        return this;
    }

    public Query<T> limit(int limit) {
        if (limit > 0) {
            buffer.putVarUInt32(Consts.QUERY_LIMIT)
                    .putVarUInt32(limit);
        }
        return this;
    }

    public Query<T> offset(int offset) {
        if (offset > 0) {
            buffer.putVarUInt32(Consts.QUERY_OFFSET)
                    .putVarUInt32(offset);
        }
        return this;
    }

    /**
     * Apply sort order to returned from query items. If values argument specified, then items equal to values, if found
     * will be placed in the top positions. For composite indexes values must be []interface{}, with value of each
     * subindex
     */
    public Query<T> sort(String index, boolean desc, Object... values) {

        buffer.putVarUInt32(Consts.QUERY_SORT_INDEX)
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
     * When fetchCount <= 0 query will fetch all results in one operation
     *
     * @param fetchCount items count to fetch
     */
    public Query<T> fetchCount(int fetchCount) {
        this.fetchCount = fetchCount;
        return this;
    }

    private void putValue(Object value) {
        if (value == null) {
            buffer.putVarUInt32(VALUE_NULL);
        } else if (value instanceof Integer) {
            buffer.putVarUInt32(Consts.VALUE_INT)
                    .putVarInt64((Integer) value);
        } else if (value instanceof String) {
            buffer.putVarUInt32(Consts.VALUE_STRING)
                    .putVString((String) value);
        }
    }

    /**
     * Will execute query, and return slice of items.
     *
     * @return an iterator over a query result
     */
    public CloseableIterator<T> execute() {
        buffer.putVarUInt32(Consts.QUERY_END);

        QueryResult queryResult = binding.selectQuery(buffer.bytes(), true, fetchCount);

        return CprotoIterator.<T>builder()
                .asJson(true)
                .binding(binding)
                .fetchCount(fetchCount)
                .namespace(namespace)
                .queryResult(queryResult)
                .build();
    }

    /**
     * Will execute query, and delete items, matches query
     *
     * @return number of deleted elements
     */
    public long delete() {
        return binding.deleteQuery(buffer.bytes());
    }

    /**
     * Adds update field request for update query
     *
     * @param fieldName field name
     * @param value     updated value
     */
    public Query<T> set(String fieldName, Object value) {
        int cmd = QUERY_UPDATE_FIELD;
        if (value instanceof Collection<?>) { //Not tested
            Collection<?> values = (Collection<?>) value;
            if (values.size() <= 0) {
                cmd = QUERY_UPDATE_FIELD_V2;
                buffer.putVarUInt32(0); //isArray
            }
            buffer.putVarUInt32(cmd);
            buffer.putVString(fieldName);
            buffer.putVarUInt32(values.size());
            for (Object v : values) {
                putValue(v);
            }
        } else if (value != null && value.getClass().isArray()) { //not tested
            Object[] values = (Object[]) value;
            if (values.length <= 0) {
                cmd = QUERY_UPDATE_FIELD_V2;
                buffer.putVarUInt32(0); //isArray
            }
            buffer.putVarUInt32(cmd);
            buffer.putVString(fieldName);
            buffer.putVarUInt32(values.length);
            for (Object v : values) {
                putValue(v);
            }
        } else {
            buffer.putVarUInt32(cmd);
            buffer.putVString(fieldName);
            buffer.putVarUInt32(1); //size
            buffer.putVarUInt32(0); //function/value flag
            putValue(value);
        }

        return this;
    }

    /**
     * Drop removes field from item within Update statement.
     *
     * @param field field to drop
     */
    public Query<T> drop(String field) {
        buffer.putVarUInt32(QUERY_DROP_FIELD);
        buffer.putVString(field);
        return this;
    }

    /**
     * Will execute query, and update fields in items, which matches query
     *
     * @return number of deleted elements
     */
    public long update() {
        return binding.updateQuery(buffer.bytes());
    }
}
