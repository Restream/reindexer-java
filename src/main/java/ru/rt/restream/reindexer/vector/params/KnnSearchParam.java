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

import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;

/**
 * Common interface for KNN search parameters.
 */
public interface KnnSearchParam {
    /**
     * K - the maximum number of documents returned from the index for subsequent filtering.
     *
     * <p>Only required parameter for all vector index types.
     */
    int getK();

    /**
     * Utility method for serializing KNN parameters to CJSON avoiding switch.
     */
    void serializeBy(ByteBuffer buffer);
}
