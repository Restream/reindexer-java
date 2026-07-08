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

import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;

import java.util.Objects;

/**
 * Represents a string expression e.g., {@code now() - 1 * 24 * 60 * 60}.
 */
public final class StringExpression implements SetExpression {

    private final String expressionString;

    /**
     * Creates an instance.
     *
     * @param expressionString the expression string to use
     */
    StringExpression(String expressionString) {
        this.expressionString = Objects.requireNonNull(expressionString, "expressionString cannot be null");
    }

    @Override
    public void serializeSet(ByteBuffer buffer) {
        buffer.putVarUInt32(1); // size.
        buffer.putVarUInt32(1); // is expression.
        buffer.putVarUInt32(Consts.VALUE_STRING);
        buffer.putVString(expressionString);
    }

    @Override
    public String toString() {
        return expressionString;
    }

}
