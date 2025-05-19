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
import ru.rt.restream.reindexer.annotations.Reindex;
import ru.rt.restream.reindexer.annotations.Transient;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.binding.cproto.ItemWriter;
import ru.rt.restream.reindexer.binding.cproto.cjson.encdec.CjsonEncoder;
import ru.rt.restream.reindexer.convert.FieldConverter;
import ru.rt.restream.reindexer.convert.FieldConverterRegistryFactory;
import ru.rt.restream.reindexer.util.BeanPropertyUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.List;
import java.util.UUID;

/**
 * Encodes item into cjson format and writes its data into the {@link ByteBuffer}.
 */
public class CJsonItemWriter<T> implements ItemWriter<T> {

    private final CtagMatcher ctagMatcher;

    public CJsonItemWriter(CtagMatcher ctagMatcher) {
        this.ctagMatcher = ctagMatcher;
    }

    @Override
    public void writeItem(ByteBuffer buffer, T item) {
        CjsonEncoder cjsonEncoder = new CjsonEncoder(ctagMatcher);
        byte[] itemData = cjsonEncoder.encode(toCjson(item, CJsonItemWriter::defaultExtract));
        buffer.writeBytes(itemData);
    }

    private CjsonElement toCjson(Object source, AnnotationExtractor annotationExtractor) {
        if (source == null) {
            return CjsonNull.INSTANCE;
        }

        if (source instanceof Integer) {
            return new CjsonPrimitive((((Integer) source).longValue()));
        } else if (source instanceof Long) {
            return new CjsonPrimitive(((Long) source));
        } else if (source instanceof Short) {
            return new CjsonPrimitive(((Short) source).longValue());
        } else if (source instanceof Byte) {
            return new CjsonPrimitive(((Byte) source).longValue());
        } else if (source instanceof Boolean) {
            return new CjsonPrimitive(((Boolean) source));
        } else if (source instanceof String) {
            return new CjsonPrimitive(((String) source));
        } else if (source instanceof Double) {
            return new CjsonPrimitive(((Double) source));
        } else if (source instanceof Float) {
            return new CjsonPrimitive((Float) source);
        } else if (source instanceof UUID) {
            return new CjsonPrimitive((UUID) source);
        } else if (source instanceof Enum<?>) {
            Enumerated enumerated = annotationExtractor.extract(Enumerated.class);
            if (enumerated != null && enumerated.value() == EnumType.STRING) {
                return new CjsonPrimitive(((Enum<?>) source).name());
            }
            int ordinal = ((Enum<?>) source).ordinal();
            return new CjsonPrimitive((long) ordinal);
        } else if (source instanceof Iterable<?>) {
            CjsonArray cjsonArray = new CjsonArray();
            for (Object element : (Iterable<?>) source) {
                CjsonElement cjsonElement = toCjson(element, annotationExtractor);
                cjsonArray.add(cjsonElement);
            }
            return cjsonArray;
        } else if (source.getClass().isArray()) {
            int length = Array.getLength(source);
            CjsonArray cjsonArray = new CjsonArray();
            for (int i = 0; i < length; i++) {
                cjsonArray.add(toCjson(Array.get(source, i), annotationExtractor));
            }
            return cjsonArray;
        } else {
            CjsonObject cjsonObject = new CjsonObject();
            List<Field> fields = BeanPropertyUtils.getInheritedFields(source.getClass());
            for (Field field : fields) {
                if (field.isAnnotationPresent(Transient.class)) {
                    continue;
                }
                Object fieldValue = readFieldValue(source, field);
                FieldConverter<Object, ?> converter = FieldConverterRegistryFactory.INSTANCE.getFieldConverter(field);
                if (converter != null) {
                    fieldValue = converter.convertToDatabaseType(fieldValue);
                }
                if (fieldValue != null) {
                    CjsonElement cjsonElement;
                    // hack for serialization of String field with Reindex.isUuid() == true as UUID.
                    if (fieldValue instanceof String && field.isAnnotationPresent(Reindex.class)
                            && field.getAnnotation(Reindex.class).isUuid()) {
                        cjsonElement = new CjsonPrimitive(UUID.fromString((String) fieldValue));
                    } else {
                        cjsonElement = toCjson(fieldValue, field::getAnnotation);
                    }
                    Json json = field.getAnnotation(Json.class);
                    String tagName = json == null ? field.getName() : json.value();
                    cjsonObject.add(tagName, cjsonElement);
                }
            }
            return cjsonObject;
        }
    }

    private Object readFieldValue(Object source, Field field) {
        return BeanPropertyUtils.getProperty(source, field.getName());
    }

    private interface AnnotationExtractor {
        <A extends Annotation> A extract(Class<A> annotationClass);
    }

    private static <A extends Annotation> A defaultExtract(Class<A> annotationClass) {
        return null;
    }
}
