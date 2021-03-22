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
package ru.rt.restream.reindexer.binding.cproto.json;

import ru.rt.restream.reindexer.binding.cproto.ItemSerializer;

import java.nio.charset.StandardCharsets;

/**
 * Converts json-formatted item to array of bytes.
 */
public class JsonItemSerializer<T> implements ItemSerializer<T> {

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] serialize(T item) {
        return ((String) item).getBytes(StandardCharsets.UTF_8);
    }

}
