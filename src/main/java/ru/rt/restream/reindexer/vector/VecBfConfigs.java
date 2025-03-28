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


import ru.rt.restream.reindexer.annotations.VecBf;

/**
 * Create the float vector search configuration for VEC_BF index from VecBf annotation.
 */

public class VecBfConfigs {
    public static final IllegalArgumentException METRIC_NULL_EX =
            new IllegalArgumentException("'metric' parameter in VEC_BF index is not set.");
    public static final IllegalArgumentException DIMENSION_ZERO_EX
            = new IllegalArgumentException("'dimension' parameter in VEC_BF index is not set.");

    public static VecBfConfig of(VecBf annotation) {
        if (annotation.metric() == null) {
            throw METRIC_NULL_EX;
        }
        if (annotation.dimension() <= 0) {
            throw DIMENSION_ZERO_EX;
        }

        VecBfConfig config = new VecBfConfig();
        config.setMetric(annotation.metric().getName());
        config.setDimension(annotation.dimension());
        config.setStartSize(annotation.startSize() < 1 ? 1000 : annotation.startSize());
        return config;
    }
}
