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
package ru.rt.restream.reindexer.util;

import java.util.Objects;

/**
 * A pair consisting of two elements.
 *
 * @param <FIRST>  first element type
 * @param <SECOND> second element type
 */
public final class Pair<FIRST, SECOND> {
    private final FIRST first;
    private final SECOND second;

    /**
     * Constructs new pair.
     *
     * @param first  first element
     * @param second second element
     */
    public Pair(FIRST first, SECOND second) {
        this.first = first;
        this.second = second;
    }

    /**
     * Get the first element from this pair.
     *
     * @return the first element of this pair
     */
    public FIRST getFirst() {
        return first;
    }

    /**
     * Get the second element from this pair.
     *
     * @return the second element of this pair
     */
    public SECOND getSecond() {
        return second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pair<?, ?> pair = (Pair<?, ?>) o;
        return Objects.equals(first, pair.first) &&
                Objects.equals(second, pair.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }
}
