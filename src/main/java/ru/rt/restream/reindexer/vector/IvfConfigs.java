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


import ru.rt.restream.reindexer.annotations.Ivf;

/**
 * Create the float vector search configuration for IVF index from Ivf annotation.
 */

public class IvfConfigs {
    public static final IllegalArgumentException METRIC_NULL_EX =
            new IllegalArgumentException("'metric' parameter in IVF index is not set.");
    public static final IllegalArgumentException DIMENSION_ZERO_EX =
            new IllegalArgumentException("'dimension' parameter in IVF index is not set.");
    public static final IllegalArgumentException CENTROID_COUNT_NOT_IN_RANGE_EX =
            new IllegalArgumentException("IVF index should have 'centroidsCount' parameter in range[1, 65536]");

    public static IvfConfig of(Ivf annotation) {
        if (annotation.metric() == null) {
            throw METRIC_NULL_EX;
        }
        if (annotation.dimension() <= 0) {
            throw DIMENSION_ZERO_EX;
        }
        int centroidsCount = annotation.centroidsCount();
        if (centroidsCount < 1 || centroidsCount > 65536) {
            throw CENTROID_COUNT_NOT_IN_RANGE_EX;
        }

        IvfConfig config = new IvfConfig();
        config.setMetric(annotation.metric().getName());
        config.setDimension(annotation.dimension());
        config.setCentroidsCount(centroidsCount);
        return config;
    }
}
