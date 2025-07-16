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
package ru.rt.restream.reindexer.vector.params;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;

import java.util.ArrayList;
import java.util.List;

import static ru.rt.restream.reindexer.binding.Consts.KNN_QUERY_PARAMS_VERSION;
import static ru.rt.restream.reindexer.binding.Consts.KNN_QUERY_TYPE_BASE;

/**
 * Common parameters for all types of KNN indices.
 *
 * <p>If all parameters are specified, the filtering will be performed in such a way that all conditions are met.
 * At least one of these parameters must be specified.
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class BaseKnnSearchParam implements KnnSearchParam {
    private static final int KNN_SERIALIZE_WITH_K = 1;
    private static final int KNN_SERIALIZE_WITH_RADIUS = 1 << 1;

    /**
     * The maximum number of documents returned from the index for subsequent filtering.
     */
    private final Integer k;
    /**
     * Parameter for filtering vectors by ranks.
     *
     * <p>Rank() < radius for L2 metrics and rank() > radius for cosine and inner product metrics.
     * About default values and usage see
     * <a href="https://reindexer.io/reindexer-docs/select/vector_search/float_vector/#range-search">
     */
    private final Float radius;

    /**
     * {@inheritDoc}
     */
    @Override
    public void serializeBy(ByteBuffer buffer) {
        buffer.putVarUInt32(KNN_QUERY_TYPE_BASE)
                .putVarUInt32(KNN_QUERY_PARAMS_VERSION);
        serializeKAndRadius(buffer);
    }

    void serializeKAndRadius(ByteBuffer buffer) {
        checkValues();
        int mask = 0;
        if (k != null) {
            mask |= KNN_SERIALIZE_WITH_K;
        }
        if (radius != null) {
            mask |= KNN_SERIALIZE_WITH_RADIUS;
        }
        buffer.putUInt8(mask);
        if (k != null) {
            buffer.putVarUInt32(k);
        }
        if (radius != null) {
            buffer.putFloat(radius);
        }
    }

    private void checkValues() {
        if (k == null && radius == null) {
            throw new IllegalArgumentException("Both params (k and radius) cannot be null");
        }
        if (k != null && k <= 0) {
            throw new IllegalArgumentException("'k' must be greater than 0");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> toLog() {
        List<String> values = new ArrayList<>(2);
        if (k != null) {
            values.add("k=" + k);
        }
        if (radius != null) {
            values.add("radius=" + radius);
        }
        return values;
    }
}
