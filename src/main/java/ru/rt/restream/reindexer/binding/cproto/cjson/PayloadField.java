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

import java.util.List;

/**
 * Payload type field descriptor.
 */
public class PayloadField {

    private final long type;

    private final String name;

    private final long offset;

    private final long size;

    private final boolean isArray;

    private final List<String> jsonPaths;

    private final int floatVectorDimension;

    /**
     * Creates an instance.
     *
     * @param type      numeric value of the field type. {@link ru.rt.restream.reindexer.binding.Consts}
     * @param name      field name
     * @param offset    field offset
     * @param size      field size
     * @param isArray   true, if field is array
     * @param jsonPaths json paths to that field
     */
    public PayloadField(long type, String name, long offset, long size, boolean isArray, List<String> jsonPaths, int floatVectorDimension) {
        this.type = type;
        this.name = name;
        this.offset = offset;
        this.size = size;
        this.isArray = isArray;
        this.jsonPaths = jsonPaths;
        this.floatVectorDimension = floatVectorDimension;
    }

    /**
     * Returns numeric value of the field type. {@link ru.rt.restream.reindexer.binding.Consts}
     *
     * @return numeric value of the field type
     */
    public long getType() {
        return type;
    }

    /**
     * Returns field name.
     *
     * @return field name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns field offset.
     *
     * @return field offset
     */
    public long getOffset() {
        return offset;
    }

    /**
     * Returns field size.
     *
     * @return field size
     */
    public long getSize() {
        return size;
    }

    /**
     * Returns true, if field is array.
     *
     * @return true, if field is array
     */
    public boolean isArray() {
        return isArray;
    }

    /**
     * Returns field json paths.
     *
     * @return field json paths
     */
    public List<String> getJsonPaths() {
        return jsonPaths;
    }
}
