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
package ru.rt.restream.reindexer.vector;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import ru.rt.restream.reindexer.annotations.Metric;

/**
 * Contains the float vector search configuration for IVF index.
 * Has no public constructor, values of fields set with {@link ru.rt.restream.reindexer.annotations.Ivf} annotation.
 * About default values and usage see
 * <a href="https://github.com/Restream/reindexer/blob/master/float_vector.md#ivf-options">
 * IVF options</a>
 */
@Setter
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PACKAGE)
public class VecBfConfig {
    /**
     * Type of metrics for calculating the measure of similarity of vectors. It should be explicitly specified.
     */
    private String metric;

    /**
     * Dimension of vector. It should be explicitly specified.
     */
    private int dimension;

    /**
     * Start size of index. Optional.
     */
    private int startSize;

}
