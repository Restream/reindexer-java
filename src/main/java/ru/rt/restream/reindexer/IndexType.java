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
package ru.rt.restream.reindexer;

/**
 * Reindexer index type.
 */
public enum IndexType {

    /**
     * Fast select by EQ and SET match. Used by default. Allows slow and ineffecient sorting by field.
     */
    HASH("hash"),

    /**
     * Fast select by RANGE, GT, and LT matches. A bit slower for EQ and SET matches than hash index.
     * Allows fast sorting results by field.
     */
    TREE("tree"),

    /**
     * Full text search index.
     */
    TEXT("text"),

    /**
     * TTL index that works only with int64 fields. These indexes are quite convenient for representation of date fields
     * (stored as UNIX timestamps) that expire after specified amount of seconds.
     */
    TTL("ttl"),

    /**
     * Available only DWITHIN match. Acceptable only for [2]double field type.
     */
    RTREE("rtree"),

    /**
     * Column index. Can't perform fast select because it's implemented with full-scan technique.
     * Has the smallest memory overhead.
     */
    COLUMN("-"),

    /**
     * Default index type will be used.
     */
    DEFAULT(null);

    private final String name;

    IndexType(String name) {
        this.name = name;
    }

    /**
     * Get the index type name.
     */
    public String getName() {
        return name;
    }

}
