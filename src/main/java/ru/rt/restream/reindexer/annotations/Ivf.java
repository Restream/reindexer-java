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
 * Use the @Ivf annotation in code to tune the ANN search params of float vector index.
 * Use it only in conjunction with {@link Reindex} annotation of IVF type.
 * About default values and usage see
 * <a href="https://github.com/Restream/reindexer/blob/master/float_vector.md#ivf-options">
 * IVF options</a>
 * Does not check values for out of bounds, if you used too small or too big value,
 * you have exception when you add or update the index.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Ivf {
    /**
     * Type of metrics for calculating the measure of similarity of vectors. It should be explicitly specified.
     */
    Metric metric();

    /**
     * Dimension of vector. It should be explicitly specified.
     */
    int dimension();

    /**
     * The number of vectors (called centroids) chosen to partition the entire set of vectors into clusters.
     *
     * <p>Required, range of values is [1, 65536].
     * <p>The higher the centroids_count, the fewer vectors each cluster contains,
     * this will speed up the search but will slow down the index building.
     * It is recommended to take values of the order between '4 * sqrt(numOfVectorsInTheIndex)'
     * and '16 * sqrt(numOfVectorsInTheIndex)'.
     */
    int centroidsCount() default 16;

}
