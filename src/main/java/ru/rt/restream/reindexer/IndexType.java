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

import java.util.Arrays;

public enum  IndexType {
    HASH("hash"), TREE("tree"), TEXT("text"), TTL("ttl"), RTREE("rtree"), COLUMN("-"), DEFAULT(null);

    private final String name;

    IndexType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static IndexType fromName(String name) {
        return Arrays.stream(IndexType.values())
                .filter(type -> type.name.equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No such index"));
    }
}
