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
package ru.rt.restream.reindexer.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import ru.rt.restream.reindexer.annotations.Json;
import ru.rt.restream.reindexer.annotations.Transient;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Converts object to json string.
 */
public class JsonSerializer {

    public static String toJson(Object src) {
        JsonElement jsonElement = toJsonElement(src);
        return jsonElement.toString();
    }

    private static JsonElement toJsonElement(Object src) {
        if (src == null) {
            return JsonNull.INSTANCE;
        }

        if (isPrimitive(src)) {
            if (src instanceof String) {
                return new JsonPrimitive((String) src);
            } else if (src instanceof Number) {
                return new JsonPrimitive((Number) src);
            } else if (src instanceof Boolean) {
                return new JsonPrimitive((Boolean) src);
            } else if (src instanceof Character) {
                return new JsonPrimitive((Character) src);
            }
        }

        if (isArray(src)) {
            Object[] array = (Object[]) src;
            JsonArray jsonArray = new JsonArray();
            for (Object element : array) {
                JsonElement jsonElement = toJsonElement(element);
                jsonArray.add(jsonElement);
            }

            return jsonArray;
        }

        if (src instanceof Collection<?>) {
            Collection<?> collection = (Collection<?>) src;
            JsonArray jsonArray = new JsonArray();
            for (Object element : collection) {
                JsonElement jsonElement = toJsonElement(element);
                jsonArray.add(jsonElement);
            }

            return jsonArray;
        }

        JsonObject jsonObject = new JsonObject();
        Collection<Field> fields = BeanPropertyUtils.getInheritedFields(src.getClass()).stream()
                .filter(field -> !field.isAnnotationPresent(Transient.class))
                .collect(Collectors.toList());
        for (Field field : fields) {
            Json json = field.getAnnotation(Json.class);
            Object property = BeanPropertyUtils.getProperty(src, field.getName());
            jsonObject.add(json != null ? json.value() : field.getName(), toJsonElement(property));
        }

        return jsonObject;
    }

    private static boolean isArray(Object src) {
        return src.getClass().isArray();
    }

    private static boolean isPrimitive(Object o) {
        return o instanceof String
               || o instanceof Boolean
               || o instanceof Number
               || o instanceof Character;
    }

}
