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

import java.util.Objects;

/**
 * Represents a {@code flat_array_len(field_name)} function expression.
 */
public final class FlatArrayLengthFunction extends FunctionExpression {

    private static final int FUNCTION_TYPE = 0;

    private final String fieldName;

    /**
     * Creates an instance.
     *
     * @param fieldName the field name to use
     */
    FlatArrayLengthFunction(String fieldName) {
        this.fieldName = Objects.requireNonNull(fieldName, "fieldName cannot be null");
    }

    @Override
    String[] getFields() {
        return new String[]{fieldName};
    }

    @Override
    int getFunctionType() {
        return FUNCTION_TYPE;
    }

    @Override
    public String toString() {
        return "flat_array_len(" + fieldName + ')';
    }

}
