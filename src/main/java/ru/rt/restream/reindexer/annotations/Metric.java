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

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Types of metrics for calculating the measure of similarity of vectors.
 */
@Getter
@RequiredArgsConstructor
public enum Metric {
    /**
     * L2 similarity is the Euclidean distance between the vectors: | v1 - v2 |.
     */
    L2("l2"),

    /**
     * Inner product similarity calculates as: 1.0 - | v1 x v2 |.
     */
    INNER_PRODUCT("inner_product"),

    /**
     * Cosine similarity is the cosine of the angle between the vectors.
     *
     * <p>In other words, it is the dot product of the vectors divided by the product of their lengths:
     * (v1 ∙ v2) / (|v1| |v2|).
     */
    COSINE("cosine");

    private final String name;
}
