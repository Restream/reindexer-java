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

import ru.rt.restream.reindexer.TimeUnit;

import java.util.Objects;

/**
 * Represents {@code now(timeUnit)} function expression.
 *
 * @see Expression#now()
 * @see Expression#now(String)
 * @see Expression#now(TimeUnit)
 */
public final class NowFunction extends FunctionExpression {

    private static final int FUNCTION_TYPE = 1;

    private final TimeUnit unit;

    /**
     * Creates an instance.
     *
     * @param unit the {@link TimeUnit} to use
     */
    NowFunction(TimeUnit unit) {
        this.unit = Objects.requireNonNull(unit, "unit cannot be null");
    }

    @Override
    Object[] getArguments() {
        return new Object[]{unit.getName()};
    }

    @Override
    int getFunctionType() {
        return FUNCTION_TYPE;
    }

    @Override
    public String toString() {
        return "now(" + unit.getName() + ')';
    }

}
