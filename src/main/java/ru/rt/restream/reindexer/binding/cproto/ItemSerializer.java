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
package ru.rt.restream.reindexer.binding.cproto;

import ru.rt.restream.reindexer.binding.cproto.cjson.CjsonItemSerializer;
import ru.rt.restream.reindexer.binding.cproto.cjson.PayloadType;
import ru.rt.restream.reindexer.binding.cproto.json.JsonItemSerializer;

/**
 * An interface for converting items to bytes.
 */
public interface ItemSerializer<T> {


    /**
     * Return ItemSerializer.
     *
     * @param itemClass   type of item to serialize: String for Json, others for Cjson.
     * @param payloadType payload type to read
     * @return ItemSerializer, that can serialize the type of item to array of bytes.
     */
    static <T> ItemSerializer<T> getInstance(Class<?> itemClass, PayloadType payloadType) {
        return itemClass == String.class
                ? (ItemSerializer<T>) JsonItemSerializer.INSTANCE
                : new CjsonItemSerializer<>(payloadType);
    }

    /**
     * Serialize item to array of bytes.
     *
     * @param item item to serialize
     * @return byte array, that contains serialized item data
     */
    byte[] serialize(T item);

}
