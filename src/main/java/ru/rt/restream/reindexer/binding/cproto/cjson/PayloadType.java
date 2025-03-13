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
package ru.rt.restream.reindexer.binding.cproto.cjson;

import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reindexer item type descriptor.
 */
@Getter
public class PayloadType {

    private final long namespaceId;

    private final String namespaceName;

    private final long version;

    private final int stateToken;

    /**
     * The payload type header offset.
     */
    private final long pStringHdrOffset;

    private final List<String> tags;

    private final List<PayloadField> fields;

    private final Map<String, Integer> names = new HashMap<>();

    /**
     * Creates an instance.
     *
     * @param namespaceId      the namespace id
     * @param namespaceName    the namespace name
     * @param version          payload type version
     * @param stateToken       payload type state token
     * @param pStringHdrOffset the payload header offset
     * @param tags             the payload tags
     * @param fields           the payload field descriptors
     */
    public PayloadType(long namespaceId, String namespaceName, long version, int stateToken, long pStringHdrOffset,
                       List<String> tags, List<PayloadField> fields) {
        this.namespaceId = namespaceId;
        this.namespaceName = namespaceName;
        this.version = version;
        this.stateToken = stateToken;
        this.pStringHdrOffset = pStringHdrOffset;
        this.tags = tags;
        this.fields = fields;
        for (int i = 0; i < tags.size(); i++) {
            names.put(tags.get(i), i);
        }
    }

}

