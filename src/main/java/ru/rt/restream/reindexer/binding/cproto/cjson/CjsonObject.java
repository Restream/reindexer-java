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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A class representing an object type in Cjson. An object consists of name-value pairs where names
 * are strings, and values are any other type of {@link CjsonElement}. This allows for a creating a
 * tree of CjsonElements.
 */
public class CjsonObject extends CjsonElement {

    private final Map<String, CjsonElement> members = new HashMap<>();

    public void add(String name, CjsonElement element) {
        if (element == null) {
            element = CjsonNull.INSTANCE;
        }

        members.put(name, element);
    }

    /**
     * Returns the property with the specified name.
     *
     * @param name name of the property that is being requested.
     * @return the property matching the name. Null if no such member exists.
     */
    public CjsonElement getProperty(String name) {
        return members.getOrDefault(name, CjsonNull.INSTANCE);
    }

    /**
     * Returns a set of properties of this object.
     *
     * @return a set of properties of this object.
     */
    public Set<Map.Entry<String, CjsonElement>> entries() {
        return members.entrySet();
    }

}
