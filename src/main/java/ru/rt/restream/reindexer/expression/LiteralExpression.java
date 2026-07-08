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

import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.Objects;

/**
 * Represents a literal expression, e.g., {@code (1, 2, 3)}.
 *
 * @see Expression#values(Object...)
 */
public final class LiteralExpression implements WhereExpression {

    private static final int EXPRESSION_TYPE = 1;

    private final Object[] values;

    /**
     * Creates an instance.
     *
     * @param values the literal values to use
     */
    LiteralExpression(Object[] values) {
        this.values = Objects.requireNonNull(values, "values cannot be null");
    }

    @Override
    public void serializeWhere(ByteBuffer buffer) {
        buffer.putVarUInt32(EXPRESSION_TYPE);
        buffer.putVarUInt32(values.length);
        for (Object value : values) {
            buffer.putValue(value);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        appendValue(sb, values);
        return sb.toString();
    }

    private void appendValue(StringBuilder sb, Object value) {
        if (value instanceof String) {
            sb.append("'").append(value).append("'");
        } else if (value instanceof Iterable<?>) {
            sb.append('(');
            for (Iterator<?> it = ((Iterable<?>) value).iterator(); it.hasNext(); ) {
                Object next = it.next();
                appendValue(sb, next);
                if (it.hasNext()) {
                    sb.append(", ");
                }
            }
            sb.append(')');
        } else if (value != null && value.getClass().isArray()) {
            sb.append('(');
            int length = Array.getLength(value);
            for (int i = 0; i < length; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                appendValue(sb, Array.get(value, i));
            }
            sb.append(')');
        } else {
            sb.append(value);
        }
    }

}
