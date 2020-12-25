/**
 * Copyright 2020 Restream
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.rt.restream.reindexer.binding.cproto.cjson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Objects of this class used to match a tag's name index with it string value.
 */
public final class CtagMatcher {

    private final Map<String, Integer> names = new HashMap<>();

    private final List<String> tags = new ArrayList<>();

    private boolean updated = false;

    /**
     * Get name of the tag.
     * @param index ctag name index
     *
     * @return ctag name
     */
    public String getName(int index) {
        index = index & ((1 << 12) - 1);

        if (index == 0) {
            return "";
        }

        if (index - 1 >= tags.size()) {
            throw new IllegalArgumentException(String.format("Unknown ctag name index %d\n", index));
        }

        return tags.get(index - 1);
    }

    /**
     * Get name index.
     * @param name ctag name
     *
     * @return ctag name index
     */
    public int getIndex(String name) {
        Integer nameIndex = names.get(name);
        if (nameIndex == null) {
            tags.add(name);
            nameIndex = tags.size() - 1;
            names.put(name, nameIndex);
            updated = true;
        }

        return nameIndex + 1;
    }

    /**
     * Read the specified payload type and construct name-index map
     * @param payloadType payload type to read
     */
    public void read(PayloadType payloadType) {
        List<String> payloadTags = payloadType.getTags();
        for (int i = 0; i < payloadTags.size(); i++) {
            tags.add(payloadTags.get(i));
            names.put(payloadTags.get(i), i);
        }
    }

    public boolean isUpdated() {
        return updated;
    }

    public List<String> getTags() {
        return tags;
    }

}
