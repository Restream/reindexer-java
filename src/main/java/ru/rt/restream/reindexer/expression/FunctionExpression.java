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

import org.apache.commons.lang3.ArrayUtils;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;

/**
 * A function expression, subclasses must provide {@link #getFunctionType()}
 * and optionally {@link #getFields()} and/or {@link #getArguments()} if function
 * supports fields and/or additional arguments.
 *
 * @see FlatArrayLengthFunction
 * @see NowFunction
 */
abstract class FunctionExpression implements WhereExpression {

    private static final int EXPRESSION_TYPE = 2;

    @Override
    public final void serializeWhere(ByteBuffer buffer) {
        buffer.putVarUInt32(EXPRESSION_TYPE);
        String[] fields = getFields();
        buffer.putVarUInt32(fields.length);
        for (String field : fields) {
            buffer.putVString(field);
        }
        Object[] arguments = getArguments();
        buffer.putVarUInt32(arguments.length);
        for (Object argument : arguments) {
            buffer.putValue(argument);
        }
        buffer.putVarUInt32(getFunctionType());
    }

    /**
     * Returns field names applied to the function.
     *
     * @return the field names to use
     */
    String[] getFields() {
        return ArrayUtils.EMPTY_STRING_ARRAY;
    }

    /**
     * Returns arguments applied to the function.
     *
     * @return the arguments to use
     */
    Object[] getArguments() {
        return ArrayUtils.EMPTY_OBJECT_ARRAY;
    }

    /**
     * Returns the function type.
     *
     * @return the function type to use
     */
    abstract int getFunctionType();

}
