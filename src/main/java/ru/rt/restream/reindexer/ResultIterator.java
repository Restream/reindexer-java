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

import java.util.Iterator;
import java.util.List;

/**
 * This interface is used by iterators that use releasable resources during iteration.
 *
 * @param <E> the type of elements returned by this iterator
 * @see Iterator
 * @see AutoCloseable
 */
public interface ResultIterator<E> extends Iterator<E>, AutoCloseable {

    /**
     * Return the total count of matching documents, if {@link Query} has set reqTotal() flag, else return 0.
     */
    long getTotalCount();

    /**
     * Returns the iterator size.
     *
     * @return the iterator size
     */
    long size();

    /**
     * Returns aggregations result.
     *
     * @return aggregations result
     * */
    List<AggregationResult> aggResults();

    /**
     * Closes the iterator.
     */
    @Override
    void close();

}
