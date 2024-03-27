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

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * An internal builder to log executed {@link Query} objects.
 */
class QueryLogBuilder {

    private String namespace;
    private QueryType type = QueryType.SELECT;
    private boolean reqTotal;
    private Integer offset;
    private Integer limit;
    private final Map<Query<?>.AggregationFacetRequest, AggregateEntry> facetParams = new HashMap<>();
    private final List<QueryEntry> whereEntries = new ArrayList<>();
    private final List<JoinEntry> joinEntries = new ArrayList<>();
    private final List<QueryEntry> onEntries = new ArrayList<>();
    private final List<SortEntry> sortEntries = new ArrayList<>();
    private final List<String> selectFields = new ArrayList<>();
    private final List<AggregateEntry> aggregateEntries = new ArrayList<>();
    private final List<UpdateEntry> updateEntries = new ArrayList<>();
    private final Deque<QueryEntry> whereStack = new ArrayDeque<>();
    private final List<QueryLogBuilder> mergeQueries = new ArrayList<>();

    private static class AggregateEntry {
        private AggregateType type;
        private Integer limit;
        private Integer offset;
        private final List<String> fields = new ArrayList<>();
        private final List<SortEntry> sortEntries = new ArrayList<>();
    }

    private static class QueryEntry {
        private Operation operation;
        private String field;
        private Condition condition;
        private String secondField;
        private int joinIndex = -1;
        private final List<Object> values = new ArrayList<>();
        private final List<QueryEntry> children = new ArrayList<>();
    }

    private static class JoinEntry {
        private QueryLogBuilder joinQueryLogBuilder;
        private JoinType type;
    }

    private static class SortEntry {
        private String sortIndex;
        private boolean desc;
        private final List<Object> values = new ArrayList<>();
    }

    private static class UpdateEntry {
        private String field;
        private Object value;
        private boolean isJsonObject;
        private boolean drop;
    }

    enum QueryType {
        SELECT, UPDATE, DELETE, TRUNCATE
    }

    enum AggregateType {
        MIN, MAX, SUM, AVG, FACET, DISTINCT
    }

    enum Operation {
        OR, AND, NOT
    }

    enum Condition {
        ANY("NOT NULL"),
        EQ("="),
        LT("<"),
        LE("<="),
        GT(">"),
        GE(">="),
        RANGE("RANGE"),
        SET("IN"),
        ALLSET("ALLSET"),
        EMPTY("IS NULL"),
        LIKE("LIKE");

        private final String name;

        Condition(String name) {
            this.name = name;
        }
    }

    enum JoinType {
        LEFT_JOIN("LEFT JOIN"),
        INNER_JOIN("INNER JOIN"),
        OR_INNER_JOIN("OR INNER JOIN");

        private final String name;

        JoinType(String name) {
            this.name = name;
        }
    }

    /**
     * Get constructed sql log string.
     *
     * @return SQL-like representation of reindexer query
     */
    String getSql() {
        StringBuilder stringBuilder = new StringBuilder(type.name());

        if (type == QueryType.SELECT || type == QueryType.DELETE) {
            if (type == QueryType.SELECT) {
                stringBuilder.append(" ")
                        .append(getSelectPart());
            }
            if (reqTotal) {
                stringBuilder.append(", COUNT(*)");
            }
            stringBuilder.append(" FROM");
        }

        stringBuilder.append(" ")
                .append(namespace);

        if (!joinEntries.isEmpty()) {
            stringBuilder.append(getJoinPart());
        }

        if (type == QueryType.UPDATE) {
            stringBuilder.append(" ")
                    .append(getUpdatePart());
        }

        if (!whereEntries.isEmpty()) {
            stringBuilder.append(" WHERE ")
                    .append(getWherePart(whereEntries));
        }

        if (!mergeQueries.isEmpty()) {
            stringBuilder.append(getMergePart());
        }

        if (!sortEntries.isEmpty()) {
            stringBuilder.append(getOrderByPart(sortEntries));
        }

        if (limit != null) {
            stringBuilder.append(" LIMIT").append(" ").append(limit);
        }

        if (offset != null) {
            stringBuilder.append(" OFFSET").append(" ").append(offset);
        }

        return stringBuilder.toString();
    }

    /**
     * Add namespace name to builder.
     *
     * @param namespace namespace name
     */
    void namespace(String namespace) {
        this.namespace = namespace;
    }

    /**
     * Add query type to builder {@link QueryType}.
     *
     * @param type type of query (select, update, etc.).
     */
    void type(QueryType type) {
        this.type = type;
    }

    /**
     * Add merge to builder.
     *
     * @param mergeQueryLogBuilder {@link QueryLogBuilder} of merged query
     */
    void merge(QueryLogBuilder mergeQueryLogBuilder) {
        mergeQueries.add(mergeQueryLogBuilder);
    }

    /**
     * Add field to builder.
     *
     * @param field the field to select
     */
    void select(String field) {
        selectFields.add(field);
    }

    /**
     * Add join to builder.
     *
     * @param joinQueryLogBuilder {@link QueryLogBuilder} of joined query
     * @param joinTypeCode        code of join type. See {@link ru.rt.restream.reindexer.binding.Consts}
     */
    void join(QueryLogBuilder joinQueryLogBuilder, int joinTypeCode) {
        JoinEntry joinEntry = new JoinEntry();
        joinEntry.type = getJoinType(joinTypeCode);
        joinEntry.joinQueryLogBuilder = joinQueryLogBuilder;
        joinEntries.add(joinEntry);
        if (joinEntry.type != JoinType.LEFT_JOIN) {
            QueryEntry queryEntry = new QueryEntry();
            queryEntry.joinIndex = joinEntries.size() - 1;
            queryEntry.operation = joinEntry.type == JoinType.OR_INNER_JOIN ? Operation.OR : Operation.AND;
            if (!whereStack.isEmpty()) {
                whereStack.getLast().children.add(queryEntry);
            } else {
                whereEntries.add(queryEntry);
            }
        }
    }

    /**
     * Add on condition to query.
     *
     * @param operationCode operation code
     * @param joinField     left side field
     * @param conditionCode condition code. See {@link Query.Condition}
     * @param joinIndex     right side index name
     */
    void on(int operationCode, String joinField, int conditionCode, String joinIndex) {
        QueryEntry queryEntry = new QueryEntry();
        queryEntry.operation = getOperation(operationCode);
        queryEntry.condition = getCondition(conditionCode);
        queryEntry.values.add(joinIndex);
        queryEntry.field = joinField;
        onEntries.add(queryEntry);
    }

    /**
     * Add where condition part to builder.
     *
     * @param operationCode operation code
     * @param field         item field to which the condition applies
     * @param conditionCode condition code. See {@link Query.Condition}
     * @param values        value(s) to which the condition applies
     */
    void where(int operationCode, String field, int conditionCode, Collection<?> values) {
        QueryEntry queryEntry = new QueryEntry();
        queryEntry.operation = getOperation(operationCode);
        queryEntry.field = field;
        queryEntry.condition = getCondition(conditionCode);
        queryEntry.values.addAll(values);
        if (!whereStack.isEmpty()) {
            QueryEntry parent = whereStack.getLast();
            parent.children.add(queryEntry);
        } else {
            whereEntries.add(queryEntry);
        }
    }

    void where(int operationCode, String field, int conditionCode, Object... values) {
        QueryEntry queryEntry = new QueryEntry();
        queryEntry.operation = getOperation(operationCode);
        queryEntry.field = field;
        queryEntry.condition = getCondition(conditionCode);
        queryEntry.values.addAll(Arrays.asList(values));
        if (!whereStack.isEmpty()) {
            QueryEntry parent = whereStack.getLast();
            parent.children.add(queryEntry);
        } else {
            whereEntries.add(queryEntry);
        }
    }

    void where(int operationCode, Query<?> subquery, int conditionCode, Object... values) {
        where(operationCode, mapToString(subquery), conditionCode, values);
    }

    void whereBetweenFields(int operationCode, String firstField, int conditionCode, String secondField) {
        QueryEntry queryEntry = new QueryEntry();
        queryEntry.operation = getOperation(operationCode);
        queryEntry.field = firstField;
        queryEntry.condition = getCondition(conditionCode);
        queryEntry.secondField = secondField;
        if (!whereStack.isEmpty()) {
            QueryEntry parent = whereStack.getLast();
            parent.children.add(queryEntry);
        } else {
            whereEntries.add(queryEntry);
        }
    }

    /**
     * Add set operation to builder. Used when value is reindexer primitive (int, long, string, bool, null) or array of
     * primitives.
     *
     * @param field field to set
     * @param value new field value
     */
    void set(String field, Object value) {
        UpdateEntry updateEntry = new UpdateEntry();
        updateEntry.field = field;
        updateEntry.value = value;
        updateEntries.add(updateEntry);
    }

    /**
     * Add set operation to builder. Use when value is reindexer object (json string).
     *
     * @param field field to set
     * @param json  new field value
     */
    void setObject(String field, String json) {
        UpdateEntry updateEntry = new UpdateEntry();
        updateEntry.field = field;
        updateEntry.value = json;
        updateEntry.isJsonObject = true;
        updateEntries.add(updateEntry);
    }

    /**
     * Add set operation to builder. Use when value is list of reindexer objects(list of json strings).
     *
     * @param field field to set
     * @param jsons new field value
     */
    void setObject(String field, List<String> jsons) {
        UpdateEntry updateEntry = new UpdateEntry();
        updateEntry.field = field;
        updateEntry.value = jsons;
        updateEntry.isJsonObject = true;
        updateEntries.add(updateEntry);
    }

    /**
     * Add drop field operation to builder.
     *
     * @param field field to drop
     */
    void drop(String field) {
        UpdateEntry updateEntry = new UpdateEntry();
        updateEntry.drop = true;
        updateEntry.field = field;
        updateEntries.add(updateEntry);
    }

    /**
     * Add aggregate operation to builder. Applicable only with aggregate operations AVG, DISTINCT, MAX, MIN, SUM.
     * For FACET operations use {@link QueryLogBuilder#aggregate(Query.AggregationFacetRequest, String...)}
     *
     * @param type  type of aggregation
     * @param field aggregation field
     */
    void aggregate(AggregateType type, String field) {
        AggregateEntry aggregateEntry = new AggregateEntry();
        aggregateEntry.type = type;
        aggregateEntry.fields.add(field);
        aggregateEntries.add(aggregateEntry);
    }

    /**
     * Add a facet aggregate operation to builder. Applicable only with FACET aggregate operations. For AVG, DISTINCT,
     * MAX, MIN, SUM operations use {@link QueryLogBuilder#aggregate(AggregateType, String)}
     *
     * @param facet  facet request to add to builder
     * @param fields aggregated fields
     */
    void aggregate(Query<?>.AggregationFacetRequest facet, String... fields) {
        AggregateEntry aggregateEntry = new AggregateEntry();
        aggregateEntry.type = AggregateType.FACET;
        aggregateEntry.fields.addAll(Arrays.asList(fields));
        aggregateEntries.add(aggregateEntry);
        facetParams.put(facet, aggregateEntry);
    }

    /**
     * Add query sort entry to builder.
     *
     * @param sortIndex sort index
     * @param desc      true, if descending order
     * @param values    forced order index values
     */
    void sort(String sortIndex, boolean desc, Object... values) {
        SortEntry sortEntry = new SortEntry();
        sortEntry.sortIndex = sortIndex;
        sortEntry.desc = desc;
        sortEntry.values.addAll(Arrays.asList(values));
        sortEntries.add(sortEntry);
    }

    /**
     * Add facet sort entry to builder.
     *
     * @param facet     facet to sort
     * @param sortIndex facet field
     * @param desc      true, if descending order
     */
    void facetSort(Query<?>.AggregationFacetRequest facet, String sortIndex, boolean desc) {
        SortEntry sortEntry = new SortEntry();
        sortEntry.sortIndex = sortIndex;
        sortEntry.desc = desc;
        facetParams.get(facet).sortEntries.add(sortEntry);
    }

    /**
     * Set flag of request of total count of items.
     */
    void reqTotal() {
        this.reqTotal = true;
    }

    /**
     * Add query offset to builder.
     *
     * @param offset offset
     */
    void offset(int offset) {
        this.offset = offset;
    }

    /**
     * Add facet offset to builder.
     *
     * @param facet  facet to apply offset
     * @param offset facet offset
     */
    void facetOffset(Query<?>.AggregationFacetRequest facet, int offset) {
        facetParams.get(facet).offset = offset;
    }

    /**
     * Add query limit to builder.
     *
     * @param limit limit
     */
    void limit(int limit) {
        this.limit = limit;
    }

    /**
     * Add facet limit to builder.
     *
     * @param facet facet to apply offset
     * @param limit facet limit
     */
    void facetLimit(Query<?>.AggregationFacetRequest facet, int limit) {
        facetParams.get(facet).limit = limit;
    }

    /**
     * Open a bracket in builder.
     *
     * @param operationCode operation code, which will be applied to expression in brackets (AND, OR ...)
     */
    void openBracket(int operationCode) {
        Operation operation = getOperation(operationCode);
        QueryEntry queryEntry = new QueryEntry();
        queryEntry.operation = operation;
        if (!whereStack.isEmpty()) {
            QueryEntry parent = whereStack.getLast();
            parent.children.add(queryEntry);
        } else {
            whereEntries.add(queryEntry);
        }
        whereStack.add(queryEntry);
    }

    /**
     * Close bracket. After closing brackets expression in brackets can not be modified.
     */
    void closeBracket() {
        whereStack.pollLast();
    }

    private Operation getOperation(int operationCode) {
        switch (operationCode) {
            case 1:
                return Operation.OR;
            case 2:
                return Operation.AND;
            case 3:
                return Operation.NOT;
            default:
                throw new RuntimeException("Illegal operation: " + operationCode);
        }
    }

    private Condition getCondition(int conditionCode) {
        switch (conditionCode) {
            case 0:
                return Condition.ANY;
            case 1:
                return Condition.EQ;
            case 2:
                return Condition.LT;
            case 3:
                return Condition.LE;
            case 4:
                return Condition.GT;
            case 5:
                return Condition.GE;
            case 6:
                return Condition.RANGE;
            case 7:
                return Condition.SET;
            case 8:
                return Condition.ALLSET;
            case 9:
                return Condition.EMPTY;
            case 10:
                return Condition.LIKE;
            default:
                throw new RuntimeException("Illegal condition: " + conditionCode);
        }
    }

    private JoinType getJoinType(int joinTypeCode) {
        switch (joinTypeCode) {
            case 0:
                return JoinType.LEFT_JOIN;
            case 1:
                return JoinType.INNER_JOIN;
            case 2:
                return JoinType.OR_INNER_JOIN;
            default:
                throw new RuntimeException("Illegal join type: " + joinTypeCode);
        }
    }

    private String getOrderByPart(List<SortEntry> sortEntries) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(" ORDER BY ");
        for (SortEntry sortEntry : sortEntries) {
            if (sortEntry != sortEntries.get(0)) {
                stringBuilder.append(", ");
            }
            if (!sortEntry.values.isEmpty()) {
                String forcedOrderValues = sortEntry.values.stream()
                        .map(String::valueOf)
                        .map(this::addQuotes)
                        .collect(Collectors.joining(", "));
                stringBuilder.append("FIELD(").append(sortEntry.sortIndex).append(", ")
                        .append(forcedOrderValues).append(")");
            } else {
                stringBuilder.append(addQuotes(sortEntry.sortIndex));
            }

            if (sortEntry.desc) {
                stringBuilder.append(" DESC");
            }
        }
        return stringBuilder.toString();
    }

    private String getJoinPart() {
        String joinPart = joinEntries.stream()
                .filter(joinEntry -> joinEntry.type == JoinType.LEFT_JOIN)
                .map(this::getSingleJoinPart)
                .collect(Collectors.joining(" "));
        return StringUtils.isNotBlank(joinPart) ? " " + joinPart : "";
    }

    private String getSingleJoinPart(JoinEntry joinEntry) {
        QueryLogBuilder joinQueryLogBuilder = joinEntry.joinQueryLogBuilder;
        JoinType type = joinEntry.type;
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(type.name)
                .append(" ")
                .append(joinQueryLogBuilder.whereEntries.isEmpty() ? joinQueryLogBuilder.namespace
                        : "(" + joinQueryLogBuilder.getSql() + ")")
                .append(" ON ");
        if (joinQueryLogBuilder.onEntries.size() > 1) {
            stringBuilder.append("(");
        }
        for (QueryEntry onEntry : joinQueryLogBuilder.onEntries) {
            if (onEntry != joinQueryLogBuilder.onEntries.get(0)) {
                stringBuilder.append(" ").append(onEntry.operation).append(" ");
            }
            stringBuilder.append(joinQueryLogBuilder.namespace)
                    .append(".").append(onEntry.values.get(0))
                    .append(" ").append(onEntry.condition.name)
                    .append(" ").append(namespace).append(".").append(onEntry.field);
        }
        if (joinQueryLogBuilder.onEntries.size() > 1) {
            stringBuilder.append(")");
        }
        return stringBuilder.toString();
    }

    private String getMergePart() {
        StringBuilder stringBuilder = new StringBuilder();
        for (QueryLogBuilder mergeQuery : mergeQueries) {
            stringBuilder.append(" MERGE(").append(mergeQuery.getSql()).append(")");
        }
        return stringBuilder.toString();
    }

    private String getWherePart(List<QueryEntry> whereEntries) {
        StringBuilder stringBuilder = new StringBuilder();
        for (QueryEntry whereEntry : whereEntries) {
            if (whereEntry.joinIndex != -1) {
                JoinEntry joinEntry = joinEntries.get(whereEntry.joinIndex);
                if (whereEntry != whereEntries.get(0)) {
                    if (joinEntry.type != JoinType.OR_INNER_JOIN) {
                        stringBuilder.append(" ").append(whereEntry.operation);
                    }
                    stringBuilder.append(" ");
                }
                stringBuilder.append(getSingleJoinPart(joinEntry));
            } else if (!whereEntry.children.isEmpty()) {
                if (whereEntry != whereEntries.get(0)) {
                    stringBuilder.append(" ")
                            .append(whereEntry.operation)
                            .append(" ");
                }
                stringBuilder.append("(").append(getWherePart(whereEntry.children))
                        .append(")");
            } else {
                if (whereEntry != whereEntries.get(0)) {
                    stringBuilder.append(" ").append(whereEntry.operation).append(" ");
                }
                stringBuilder.append(whereEntry.field)
                        .append(" ").append(whereEntry.condition.name);
                if (whereEntry.secondField != null) {
                    stringBuilder.append(" ").append(whereEntry.secondField);
                } else if (whereEntry.values.size() == 1) {
                    Object value = whereEntry.values.get(0);
                    stringBuilder.append(" ").append(mapToString(value));
                } else if (whereEntry.values.size() > 1) {
                    stringBuilder.append(" (");
                    String logValues = whereEntry.values.stream()
                            .map(this::mapToString)
                            .collect(Collectors.joining(", "));
                    stringBuilder.append(logValues).append(")");
                }
            }
        }
        return stringBuilder.toString();
    }

    private String mapToString(Object whereEntryValue) {
        if (whereEntryValue.getClass().isArray()) {
            return Arrays.stream((Object[]) whereEntryValue)
                    .map(v -> v instanceof String ? addQuotes(v) : String.valueOf(v))
                    .collect(Collectors.joining(", ", "{", "}"));
        } else if (whereEntryValue instanceof Query<?>) {
            Query<?> subquery = (Query<?>) whereEntryValue;
            return "(" + subquery.getSql() + ")";
        }
        return whereEntryValue instanceof String ? addQuotes(whereEntryValue) : String.valueOf(whereEntryValue);
    }

    private String getSelectPart() {
        if (!aggregateEntries.isEmpty()) {
            return aggregateEntries.stream()
                    .map(this::getAggregationLogValue)
                    .collect(Collectors.joining(", "));
        }
        if (!selectFields.isEmpty()) {
            return String.join(", ", selectFields);
        }
        return "*";
    }

    private String getUpdatePart() {
        Map<Boolean, List<UpdateEntry>> updates = updateEntries.stream()
                .collect(Collectors.groupingBy(entry -> entry.drop));
        StringBuilder stringBuilder = new StringBuilder();
        List<UpdateEntry> updateEntries = updates.get(false);
        if (updateEntries != null) {
            stringBuilder.append("SET ");
            String updateFieldsPart = updateEntries.stream()
                    .map(updateEntry -> {
                        String logValue = getValueLog(updateEntry.value, updateEntry.isJsonObject);
                        return updateEntry.field + " = " + logValue;
                    })
                    .collect(Collectors.joining(", "));
            stringBuilder.append(updateFieldsPart);
        }
        List<UpdateEntry> dropEntries = updates.get(true);
        if (dropEntries != null) {
            stringBuilder.append(" DROP ");
            String dropFieldsPart = dropEntries.stream()
                    .map(dropEntry -> dropEntry.field)
                    .collect(Collectors.joining(", "));
            stringBuilder.append(dropFieldsPart);
        }
        return stringBuilder.toString();
    }

    private String getValueLog(Object value, boolean isJsonObject) {
        if (isJsonObject) {
            return String.valueOf(value);
        }
        return (value instanceof String ? addQuotes(value) : String.valueOf(value));
    }

    private String getAggregationLogValue(AggregateEntry aggregateEntry) {
        StringBuilder stringBuilder = new StringBuilder();
        String logValue = String.join(", ", aggregateEntry.fields);
        stringBuilder.append(logValue);
        if (aggregateEntry.type == AggregateType.FACET) {
            if (!aggregateEntry.sortEntries.isEmpty()) {
                stringBuilder.append(getOrderByPart(aggregateEntry.sortEntries));
            }
            if (aggregateEntry.offset != null) {
                stringBuilder.append(" OFFSET ").append(aggregateEntry.offset);
            }
            if (aggregateEntry.limit != null) {
                stringBuilder.append(" LIMIT ").append(aggregateEntry.limit);
            }
        }
        return aggregateEntry.type.name() + "(" + stringBuilder.toString() + ")";
    }

    private String addQuotes(Object value) {
        return "'" + value + "'";
    }

}
