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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This class represents an array type in Cjson. An array is a list of {@link CjsonElement}s each of
 * which can be of a different type.
 */
public class CjsonArray extends CjsonElement implements Iterable<CjsonElement> {

    private final List<CjsonElement> members = new ArrayList<>();

    /**
     * Adds the specified element to self.
     *
     * @param element the element that needs to be added to the array.
     */
    public void add(CjsonElement element) {
        if (element == null) {
            element = CjsonNull.INSTANCE;
        }

        members.add(element);
    }

    @Override
    public Iterator<CjsonElement> iterator() {
        return members.iterator();
    }

    /**
     * Return CjsonElement list backed by the current CjsonArray.
     *
     * @return list view of the current CjsonArray
     */
    public List<CjsonElement> list() {
        return members;
    }
}
