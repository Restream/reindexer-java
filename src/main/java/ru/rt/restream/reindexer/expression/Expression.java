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
package ru.rt.restream.reindexer.expression;

import ru.rt.restream.reindexer.Query;
import ru.rt.restream.reindexer.TimeUnit;

/**
 * Represents a Reindexer expression, provides factory methods to creates different types of {@code Expression}.
 *
 * @see FieldExpression
 * @see LiteralExpression
 * @see StringExpression
 * @see NowFunction
 * @see FlatArrayLengthFunction
 * @see SubQueryExpression
 */
public interface Expression {

    /**
     * Creates a {@link FieldExpression} to use for field comparison.
     *
     * @param field the field name to use
     * @return the {@link FieldExpression} to use
     */
    static FieldExpression field(String field) {
        return new FieldExpression(field);
    }

    /**
     * Creates a {@link LiteralExpression} to use for literal values comparison.
     * <p>
     * Note: the expression expects raw values, the data type conversion is not supported.
     *
     * @param values the values to use
     * @return the {@link LiteralExpression} to use
     */
    static LiteralExpression values(Object... values) {
        return new LiteralExpression(values);
    }

    /**
     * Creates a {@link StringExpression} to use for arithmetical expressions e.g., {@code now() - 1 * 24 * 60 * 60}.
     * <p>
     * Note: the string-based expressions are currently only supported for update queries.
     *
     * @param expressionString the expression string to use
     * @return the {@link StringExpression} to use
     */
    static StringExpression string(String expressionString) {
        return new StringExpression(expressionString);
    }

    /**
     * Creates a {@link NowFunction} to use for date comparison, defaults to {@link TimeUnit#SECONDS} time unit.
     *
     * @return the {@link NowFunction} to use
     */
    static NowFunction now() {
        return now(TimeUnit.SECONDS);
    }

    /**
     * Creates a {@link NowFunction} to use for date comparison with the specified {@code timeUnit}.
     *
     * @param unit the name of {@link TimeUnit} to use
     * @return the {@link NowFunction} to use
     */
    static NowFunction now(String unit) {
        return now(TimeUnit.fromName(unit));
    }

    /**
     * Creates a {@link NowFunction} to use for date comparison with the specified {@code timeUnit}.
     *
     * @param unit the {@link TimeUnit} to use
     * @return the {@link NowFunction} to use
     */
    static NowFunction now(TimeUnit unit) {
        return new NowFunction(unit);
    }

    /**
     * Creates a {@link FlatArrayLengthFunction} to use for array comparison.
     *
     * @param fieldName the field name to use
     * @return the {@link FlatArrayLengthFunction} to use
     */
    static FlatArrayLengthFunction flatArrayLength(String fieldName) {
        return new FlatArrayLengthFunction(fieldName);
    }

    /**
     * Creates a {@link SubQueryExpression} to use for comparison.
     *
     * @param subQuery the sub {@link Query} to use
     * @return the {@link SubQueryExpression} to use
     */
    static SubQueryExpression subQuery(Query<?> subQuery) {
        return new SubQueryExpression(subQuery);
    }

}
