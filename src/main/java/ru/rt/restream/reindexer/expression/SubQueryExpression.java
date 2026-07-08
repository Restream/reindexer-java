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
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;

import java.util.Objects;

/**
 * Represents a subquery expression.
 *
 * @see Expression#subQuery(Query)
 */
public final class SubQueryExpression implements WhereExpression {

    private static final int EXPRESSION_TYPE = 3;

    private final Query<?> subQuery;

    /**
     * Creates an instance.
     *
     * @param subQuery the {@link Query subquery} to use
     */
    SubQueryExpression(Query<?> subQuery) {
        this.subQuery = Objects.requireNonNull(subQuery, "subQuery cannot be null");
    }

    @Override
    public void serializeWhere(ByteBuffer buffer) {
        buffer.putVarUInt32(EXPRESSION_TYPE);
        buffer.putVBytes(subQuery.bytes());
    }

    @Override
    public String toString() {
        return '(' + subQuery.toString() + ')';
    }

}
