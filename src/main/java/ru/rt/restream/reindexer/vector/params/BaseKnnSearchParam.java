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

import java.util.Collections;
import java.util.List;

import static ru.rt.restream.reindexer.binding.Consts.KNN_QUERY_PARAMS_VERSION;
import static ru.rt.restream.reindexer.binding.Consts.KNN_QUERY_TYPE_BASE;

@Getter
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class BaseKnnSearchParam implements KnnSearchParam {
    /**
     * The maximum number of documents returned from the index for subsequent filtering.
     */
    private final int k;

    /**
     * {@inheritDoc}
     */
    @Override
    public void serializeBy(ByteBuffer buffer) {
        buffer.putVarUInt32(KNN_QUERY_TYPE_BASE)
                .putVarUInt32(KNN_QUERY_PARAMS_VERSION)
                .putVarUInt32(k);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> toLog() {
        return Collections.singletonList("k=" + k);
    }
}
