/*
 * Copyright 2020-present Restream
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
import ru.rt.restream.reindexer.binding.definition.IndexConfig;

/**
 * Contains the float vector search configuration for HNSW index.
 * Has no public constructor, values of fields set with {@link ru.rt.restream.reindexer.annotations.Hnsw} annotation.
 * About default values and usage see
 * <a href="https://github.com/Restream/reindexer/blob/master/float_vector.md#hnsw-options">
 * HNSW options</a>
 */
@Setter
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PACKAGE)
public class HnswConfig implements IndexConfig {
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

    /**
     * The number of bidirectional links created for every new element during construction.
     */
    private int m;

    /**
     * The parameter controls the index_time/index_accuracy.
     */
    private int efConstruction;

    /**
     * Enable multithreading insert mode.
     */
    private int multithreading;

    /**
     * Optional quantization configuration for HNSW index.
     *
     * <p>
     * When this value is {@code null} the {@code quantization_config} block is not
     * serialized.
     */
    private QuantizationConfig quantizationConfig;

    @Setter
    @Getter
    @EqualsAndHashCode
    @NoArgsConstructor(access = AccessLevel.PACKAGE)
    public static class QuantizationConfig {
        /**
         * Quantization type.
         *
         * <p>
         * Currently supported: {@code scalar_quantization_8_bit}.
         */
        private String quantizationType;

        /**
         * Quantile for scalar quantization.
         *
         * <p>
         * If this field is {@code null}, quantile is expected to be computed
         * automatically
         * by Reindexer.
         */
        private Float quantile;

        /**
         * Sample size for estimating quantile(s).
         */
        private int sampleSize;

        /**
         * Minimal number of samples/points required to enable quantization.
         */
        private int quantizationThreshold;
    }
}
