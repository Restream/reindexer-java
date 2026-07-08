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

import java.util.Objects;

/**
 * Represents a field expression.
 *
 * @see Expression#field(String)
 */
public final class FieldExpression implements WhereExpression {

    private static final int EXPRESSION_TYPE = 0;

    private final String fieldName;

    /**
     * Creates an instance.
     *
     * @param fieldName the field name to use
     */
    FieldExpression(String fieldName) {
        this.fieldName = Objects.requireNonNull(fieldName, "fieldName cannot be null");
    }

    @Override
    public void serializeWhere(ByteBuffer buffer) {
        buffer.putVarUInt32(EXPRESSION_TYPE);
        buffer.putVString(fieldName);
    }

    @Override
    public String toString() {
        return fieldName;
    }

}
