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

import ru.rt.restream.reindexer.EnumType;
import ru.rt.restream.reindexer.annotations.Enumerated;
import ru.rt.restream.reindexer.annotations.Json;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.binding.cproto.ItemReader;
import ru.rt.restream.reindexer.binding.cproto.cjson.encdec.CjsonDecoder;
import ru.rt.restream.reindexer.convert.FieldConverter;
import ru.rt.restream.reindexer.convert.FieldConverterRegistryFactory;
import ru.rt.restream.reindexer.util.BeanPropertyUtils;
import ru.rt.restream.reindexer.convert.util.ConversionUtils;
import ru.rt.restream.reindexer.convert.util.ResolvableType;
import ru.rt.restream.reindexer.util.CollectionUtils;
import ru.rt.restream.reindexer.util.Pair;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

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
        FieldConverter<?, Object> converter = FieldConverterRegistryFactory.INSTANCE.getFieldConverter(field);
        if (converter != null) {
            Pair<ResolvableType, ResolvableType> convertiblePair = converter.getConvertiblePair();
            return converter.convertToFieldType(getTargetValue(field, convertiblePair.getSecond(), property));
        }
        ResolvableType resolvableType = ConversionUtils.resolveFieldType(field);
        return getTargetValue(field, resolvableType, property);
    }

    private Object getTargetValue(Field field, ResolvableType resolvableType, CjsonElement property) {
        if (property.isNull()) {
            if (resolvableType.isCollectionLike()) {
                return resolvableType.getType().isArray() ? Array.newInstance(resolvableType.getComponentType(), 0)
                        : CollectionUtils.createCollection(resolvableType.getType(), resolvableType.getComponentType(), 0);
            }
            return null;
        }

        if (resolvableType.isCollectionLike()) {
            List<CjsonElement> elements = property.getAsCjsonArray().list();
            if (resolvableType.getType().isArray()) {
                Object array = Array.newInstance(resolvableType.getComponentType(), elements.size());
                for (int i = 0; i < elements.size(); i++) {
                    Array.set(array, i, convert(elements.get(i), resolvableType.getComponentType(), field));
                }
                return array;
            }
            Collection<Object> collection = CollectionUtils
                    .createCollection(resolvableType.getType(), resolvableType.getComponentType(), elements.size());
            for (CjsonElement element : elements) {
                collection.add(convert(element, resolvableType.getComponentType(), field));
            }
            return collection;
        } else {
            return convert(property, resolvableType.getType(), field);
        }
    }

    private Object convert(CjsonElement element, Class<?> targetClass, Field field) {
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
        } else if (targetClass == UUID.class) {
            return element.getAsUuid();
        } else if (Enum.class.isAssignableFrom(targetClass)) {
            Enumerated enumerated = field.getAnnotation(Enumerated.class);
            if (enumerated != null && enumerated.value() == EnumType.STRING) {
                return Enum.valueOf(targetClass.asSubclass(Enum.class), element.getAsString());
            }
            return targetClass.getEnumConstants()[element.getAsInteger()];
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
