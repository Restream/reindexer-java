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
import lombok.NonNull;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;

import java.util.ArrayList;
import java.util.List;

import static ru.rt.restream.reindexer.binding.Consts.KNN_QUERY_PARAMS_VERSION;
import static ru.rt.restream.reindexer.binding.Consts.KNN_QUERY_TYPE_HNSW;

@Getter
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class IndexHnswSearchParam implements KnnSearchParam {
    /**
     * Common parameters for KNN search.
     */
    @NonNull
    private final BaseKnnSearchParam base;

    /**
     * The size of the dynamic list for the nearest neighbors.
     *
     * <p>Higher 'ef' leads to more accurate but slower search.
     * The value 'ef' of can be anything between k and the size of the dataset.
     * Optional, minimum and default values are 'k'
     */
    private final int ef;

    /**
     * {@inheritDoc}
     */
    @Override
    public void serializeBy(ByteBuffer buffer) {
        buffer.putVarUInt32(KNN_QUERY_TYPE_HNSW)
                .putVarUInt32(KNN_QUERY_PARAMS_VERSION);
        base.serializeKAndRadius(buffer);
        buffer.putVarInt32(ef);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> toLog() {
        List<String> values = new ArrayList<>(3);
        values.addAll(base.toLog());
        values.add("ef=" + ef);
        return values;
    }
}
