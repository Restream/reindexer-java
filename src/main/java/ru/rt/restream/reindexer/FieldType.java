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
 * Reindexer item field type.
 */
public enum FieldType {
    /**
     * Reindexer boolean type.
     */
    BOOL("bool"),

    /**
     * Reindexer integer type.
     */
    INT("int"),

    /**
     * Reindexer "long" type.
     */
    INT64("int64"),

    /**
     * Reindexer double type.
     */
    DOUBLE("double"),

    /**
     * Reindexer float type.
     */
    FLOAT("float"),

    /**
     * Reindexer string type.
     */
    STRING("string"),

    /**
     * Reindexer UUID type.
     */
    UUID("uuid"),

    /**
     * Reindexer composite type.
     */
    COMPOSITE("composite");

    private final String name;

    FieldType(String name) {
        this.name = name;
    }

    /**
     * Get the current field type name.
     */
    public String getName() {
        return name;
    }
}
