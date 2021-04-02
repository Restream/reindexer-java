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

import ru.rt.restream.reindexer.annotations.Json;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.binding.cproto.ItemReader;
import ru.rt.restream.reindexer.binding.cproto.cjson.encdec.CjsonDecoder;
import ru.rt.restream.reindexer.util.BeanPropertyUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads items from a {@link ByteBuffer}, that contains cjson-encoded data.
 */
public class CjsonItemReader<T> implements ItemReader<T> {

    private final Class<T> itemClass;

    private final CtagMatcher ctagMatcher;

    public CjsonItemReader(Class<T> itemClass, CtagMatcher ctagMatcher) {
        this.itemClass = itemClass;
        this.ctagMatcher = ctagMatcher;
    }

    @Override
    public T readItem(ByteBuffer buffer) {
        CjsonDecoder reader = new CjsonDecoder(ctagMatcher, buffer);
        CjsonElement element = reader.decode();
        if (!element.isObject()) {
            throw new IllegalArgumentException("Read object is not an item");
        }
        return readObject(element.getAsCjsonObject(), itemClass);
    }

    private <V> V readObject(CjsonObject cjsonObject, Class<V> itemClass) {
        V instance = createInstance(itemClass);
        List<Field> fields = BeanPropertyUtils.getInheritedFields(itemClass);
        for (Field field : fields) {
            Json json = field.getAnnotation(Json.class);
            String tagName = json == null ? field.getName() : json.value();
            Object value = getTargetValue(field, cjsonObject.getProperty(tagName));
            if (value != null) {
                BeanPropertyUtils.setProperty(instance, field.getName(), value);
            }
        }
        return instance;
    }

    private Object getTargetValue(Field field, CjsonElement property) {
        Class<?> fieldType = field.getType();
        if (property.isNull()) {
            if (fieldType == List.class) {
                return new ArrayList<>();
            }
            return null;
        }

        if (fieldType == List.class) {
            CjsonArray array = property.getAsCjsonArray();
            ArrayList<Object> elements = new ArrayList<>();
            ParameterizedType genericType = (ParameterizedType) field.getGenericType();
            Type elementType = genericType.getActualTypeArguments()[0];
            for (CjsonElement cjsonElement : array) {
                elements.add(convert(cjsonElement, (Class<?>) elementType));
            }
            return elements;
        } else {
            return convert(property, field.getType());
        }
    }

    private Object convert(CjsonElement element, Class<?> targetClass) {
        if (element.isNull()) {
            return null;
        } else if (targetClass == Integer.class || targetClass == int.class) {
            return element.getAsInteger();
        } else if (targetClass == Long.class || targetClass == long.class) {
            return element.getAsLong();
        } else if (targetClass == Short.class || targetClass == short.class) {
            return element.getAsShort();
        } else if (targetClass == Byte.class || targetClass == byte.class) {
            return element.getAsByte();
        } else if (targetClass == Boolean.class || targetClass == boolean.class) {
            return element.getAsBoolean();
        } else if (targetClass == String.class) {
            return element.getAsString();
        } else if (targetClass == Double.class || targetClass == double.class) {
            return element.getAsDouble();
        } else if (targetClass == Float.class || targetClass == float.class) {
            return element.getAsFloat();
        } else if (element.isObject()) {
            return readObject(element.getAsCjsonObject(), targetClass);
        } else {
            throw new UnsupportedOperationException(String.format("Unsupported data type: %s", targetClass.getName()));
        }
    }

    private static <T> T createInstance(Class<T> beanClass) {
        try {
            Constructor<T> constructor = beanClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            return constructor.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
