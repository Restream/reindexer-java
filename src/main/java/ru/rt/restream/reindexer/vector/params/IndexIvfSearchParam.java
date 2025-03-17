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

import static ru.rt.restream.reindexer.binding.Consts.KNN_QUERY_PARAMS_VERSION;
import static ru.rt.restream.reindexer.binding.Consts.KNN_QUERY_TYPE_IVF;

@Getter
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class IndexIvfSearchParam implements KnnSearchParam {
    /**
     * The maximum number of documents returned from the index for subsequent filtering.
     */
    private final int k;

    /**
     * the number of clusters to be looked at during the search.
     *
     * <p>Increasing this parameter allows you to get a higher quality result (recall rate),
     * but at the same time slows down the search.
     * Optional, should be greater than 0, default value is 1.
     */
    private final int nProbe;

    /**
     * {@inheritDoc}
     */
    @Override
    public void serializeBy(ByteBuffer buffer) {
        buffer.putVarUInt32(KNN_QUERY_TYPE_IVF)
                .putVarUInt32(KNN_QUERY_PARAMS_VERSION)
                .putVarUInt32(k)
                .putVarUInt32(nProbe);
    }
}
