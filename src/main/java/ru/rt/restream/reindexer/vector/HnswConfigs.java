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

import ru.rt.restream.reindexer.annotations.Hnsw;

/**
 * Create the float vector search configuration for HNSW index from Hnsw annotation.
 */

public class HnswConfigs {
    public static final IllegalArgumentException METRIC_NULL_EX =
            new IllegalArgumentException("'metric' parameter in HNSW index is not set.");
    public static final IllegalArgumentException DIMENSION_ZERO_EX =
            new IllegalArgumentException("'dimension' parameter in HNSW index is not set.");
    public static final IllegalArgumentException M_NOT_IN_RANGE_EX =
            new IllegalArgumentException("HNSW index should have 'm' parameter in range [2, 128]");
    public static final IllegalArgumentException EF_CONSTRUCTION_NOT_IN_RANGE_EX =
            new IllegalArgumentException("HNSW index should have 'efConstruction' parameter in range [4, 1024]");

    public static HnswConfig of(Hnsw annotation) {
        if (annotation.metric() == null) {
            throw METRIC_NULL_EX;
        }
        if (annotation.dimension() <= 0) {
            throw DIMENSION_ZERO_EX;
        }
        if (isNotInRange(annotation.m(), 2, 128)) {
            throw M_NOT_IN_RANGE_EX;
        }
        if (isNotInRange(annotation.efConstruction(), 4, 1024)) {
            throw EF_CONSTRUCTION_NOT_IN_RANGE_EX;
        }

        HnswConfig config = new HnswConfig();
        config.setMetric(annotation.metric().getName());
        config.setDimension(annotation.dimension());
        config.setStartSize(annotation.startSize() < 1 ? 1000 : annotation.startSize());
        config.setM(annotation.m());
        config.setEfConstruction(annotation.efConstruction());
        config.setMultithreading(annotation.multithreading() ? 1 : 0);
        return config;
    }

    private static boolean isNotInRange(int value, int min, int max) {
        return value < min || value > max;
    }
}
