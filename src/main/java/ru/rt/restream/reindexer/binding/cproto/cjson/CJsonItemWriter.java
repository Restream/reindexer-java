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
import ru.rt.restream.reindexer.annotations.Transient;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.binding.cproto.ItemWriter;
import ru.rt.restream.reindexer.binding.cproto.cjson.encdec.CjsonEncoder;
import ru.rt.restream.reindexer.util.BeanPropertyUtils;

import java.lang.reflect.Field;
import java.util.List;

public class CJsonItemWriter<T> implements ItemWriter<T> {

    private final CtagMatcher ctagMatcher;

    public CJsonItemWriter(CtagMatcher ctagMatcher) {
        this.ctagMatcher = ctagMatcher;
    }

    @Override
    public void writeItem(ByteBuffer buffer, T item) {
        CjsonEncoder cjsonEncoder = new CjsonEncoder(ctagMatcher);
        byte[] itemData = cjsonEncoder.encode(toCjson(item));
        buffer.writeBytes(itemData);
    }

    private CjsonElement toCjson(Object source) {
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
            return new CjsonPrimitive(((Float) source).doubleValue());
        } else if (source instanceof List) {
            CjsonArray cjsonArray = new CjsonArray();
            List<?> sourceList = (List<?>) source;
            for (Object element : sourceList) {
                CjsonElement cjsonElement = toCjson(element);
                cjsonArray.add(cjsonElement);
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
                if (fieldValue != null) {
                    CjsonElement cjsonElement = toCjson(fieldValue);
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

}
