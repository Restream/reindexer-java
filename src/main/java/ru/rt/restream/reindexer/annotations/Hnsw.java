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
package ru.rt.restream.reindexer.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Use the @Hnsw annotation in code to tune the ANN search params of float vector index.
 * Use it only in conjunction with {@link Reindex} annotation of HNSW type.
 * About default values and usage see
 * <a href="https://github.com/Restream/reindexer/blob/master/float_vector.md#hnsw-options">
 * HNSW options</a>
 * Does not check values for out of bounds, if you used too small or too big value,
 * you have exception when you add or update the index.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Hnsw {
    /**
     * Type of metrics for calculating the measure of similarity of vectors. It should be explicitly specified.
     */
    Metric metric();

    /**
     * Dimension of vector. It should be explicitly specified.
     */
    int dimension();

    /**
     *  Start size of index. Optional.
     *
     *  <p>The optimal value is equal to the size of the fully filled index.
     *  A much larger start_size value will result in memory overuse,
     *  while a much smaller start_size value will slow down inserts.
     *  Minimum and default values are 1000.
     */
    int startSize() default 1000;

    /**
     * The number of bidirectional links created for every new element during construction.
     *
     * <p>Optional, range of values is [2, 128]
     * <p>Higher 'm' work better on datasets with high intrinsic dimensionality and/or high recall,
     * while low 'm' work better for datasets with low intrinsic dimensionality and/or low recalls.
     * The parameter also determines the algorithm's memory consumption,
     * which is roughly M * 8-10 bytes per stored element.
     */
    int m() default 16;

    /**
     * The parameter controls the index_time/index_accuracy.
     *
     * <p>Optional, range of values is [4, 1024]
     * <p>Bigger 'efConstruction' leads to longer construction, but better index quality.
     * At some point, increasing ef_construction does not improve the quality of the index.
     */
    int efConstruction() default 200;

    /**
     * Enable multithreading insert mode.
     *
     * @return true, if multithreading mode enabled (uses more memory and CPU, but inserting into index is faster).
     */
    boolean multithreading() default false;

}
