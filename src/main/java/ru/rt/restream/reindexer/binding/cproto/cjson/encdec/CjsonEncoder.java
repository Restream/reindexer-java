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
package ru.rt.restream.reindexer.binding.cproto.cjson.encdec;

import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.binding.cproto.cjson.CjsonArray;
import ru.rt.restream.reindexer.binding.cproto.cjson.CjsonElement;
import ru.rt.restream.reindexer.binding.cproto.cjson.CjsonObject;
import ru.rt.restream.reindexer.binding.cproto.cjson.CjsonPrimitive;
import ru.rt.restream.reindexer.binding.cproto.cjson.CtagMatcher;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Encodes CjsonElement to a byte array.
 */
public class CjsonEncoder {

    private final ByteBuffer buffer = new ByteBuffer();
    private final CtagMatcher ctagMatcher;

    /**
     * Creates an instance.
     *
     * @param ctagMatcher item ctag matcher
     */
    public CjsonEncoder(CtagMatcher ctagMatcher) {
        this.ctagMatcher = ctagMatcher;
    }

    /**
     * This method encodes a {@link CjsonElement} tree to its equivalent representation as a byte array.
     * @param element cjson element to encode
     *
     * @return encoded CjsonElement as byte array
     */
    public byte[] encode(CjsonElement element) {
        Ctag endTag = new Ctag(Ctag.END, 0, 0);
        buffer.putVarUInt32(endTag.getValue());
        int offsetPosition = buffer.bytes().length;
        buffer.putUInt32(0L);
        encodeElement(element, 0);
        if (ctagMatcher.isUpdated()) {
            buffer.putUInt32(buffer.getPosition(), offsetPosition);
            writeUpdatedTags();
        } else {
            buffer.truncateStart(5);
        }
        return buffer.bytes();
    }

    private void writeUpdatedTags() {
        List<String> tags = ctagMatcher.getTags();
        buffer.putVarUInt32(tags.size());
        for (String tag : tags) {
            buffer.putVString(tag);
        }
    }

    private void encodeElement(CjsonElement element, int ctagName) {
        if (element.isObject()) {
            encodeObject(element.getAsCjsonObject(), ctagName);
        } else if (element.isPrimitive()) {
            encodePrimitive(element.getAsCjsonPrimitive(), ctagName);
        } else if (element.isArray()) {
            encodeArray(element.getAsCjsonArray(), ctagName);
        } else if (element.isNull()) {
            Ctag ctag = new Ctag(Ctag.NULL, ctagName, 0);
            buffer.putVarUInt32(ctag.getValue());
        }
    }

    private void encodeArray(CjsonArray cjsonArray, int ctagName) {
        List<CjsonElement> elements = cjsonArray.list();
        if (elements.size() > 0) {
            Ctag ctag = new Ctag(Ctag.ARRAY, ctagName, 0);
            buffer.putVarUInt32(ctag.getValue());
            CjsonElement cjsonElement = elements.get(0);
            int ctagType = getCtagType(cjsonElement);
            CarrayTag carrayTag = new CarrayTag(elements.size(), ctagType);
            buffer.putUInt32(carrayTag.getValue());
            if (ctagType == Ctag.VARINT) {
                for (CjsonElement element : elements) {
                    buffer.putVarInt64(element.getAsLong());
                }
            } else if (ctagType == Ctag.DOUBLE) {
                for (CjsonElement element : elements) {
                    buffer.putDouble(element.getAsDouble());
                }
            } else if (ctagType == Ctag.STRING) {
                for (CjsonElement element : elements) {
                    buffer.putVString(element.getAsString());
                }
            } else if (ctagType == Ctag.BOOL) {
                for (CjsonElement element : elements) {
                    buffer.putVarUInt32(element.getAsBoolean() ? 1L : 0L);
                }
            } else if (ctagType == Ctag.UUID) {
                for (CjsonElement element : elements) {
                    buffer.putUuid(element.getAsUuid());
                }
            } else if (ctagType == Ctag.OBJECT) {
                for (CjsonElement element : elements) {
                    encodeObject(element.getAsCjsonObject(), 0);
                }
            }
        }
    }

    private int getCtagType(CjsonElement cjsonElement) {
        if (cjsonElement.isObject()) {
            return Ctag.OBJECT;
        } else if (cjsonElement.isPrimitive()) {
            CjsonPrimitive cjsonPrimitive = cjsonElement.getAsCjsonPrimitive();
            if (cjsonPrimitive.isIntegral()) {
                return Ctag.VARINT;
            } else if (cjsonPrimitive.isDouble()) {
                return Ctag.DOUBLE;
            } else if (cjsonPrimitive.isString()) {
                return Ctag.STRING;
            } else if (cjsonPrimitive.isBoolean()) {
                return Ctag.BOOL;
            } else if (cjsonPrimitive.isUuid()) {
                return Ctag.UUID;
            }
        }
        throw new UnsupportedOperationException(String.format("Unsupported data cjson type: %s", cjsonElement));
    }

    private void encodePrimitive(CjsonPrimitive cjsonPrimitive, int ctagName) {
        if (cjsonPrimitive.isBoolean()) {
            Ctag ctag = new Ctag(Ctag.BOOL, ctagName, 0);
            Boolean value = cjsonPrimitive.getAsBoolean();
            buffer.putVarUInt32(ctag.getValue());
            buffer.putVarUInt32(value ? 1L : 0L);
        } else if (cjsonPrimitive.isDouble()) {
            Ctag ctag = new Ctag(Ctag.DOUBLE, ctagName, 0);
            Double value = cjsonPrimitive.getAsDouble();
            buffer.putVarUInt32(ctag.getValue());
            buffer.putDouble(value);
        } else if (cjsonPrimitive.isString()) {
            Ctag ctag = new Ctag(Ctag.STRING, ctagName, 0);
            String value = cjsonPrimitive.getAsString();
            buffer.putVarUInt32(ctag.getValue());
            buffer.putVString(value);
        } else if (cjsonPrimitive.isIntegral()) {
            Ctag ctag = new Ctag(Ctag.VARINT, ctagName, 0);
            Long value = cjsonPrimitive.getAsLong();
            buffer.putVarUInt32(ctag.getValue());
            buffer.putVarInt64(value);
        } else if (cjsonPrimitive.isUuid()) {
            Ctag ctag = new Ctag(Ctag.UUID, ctagName, 0);
            UUID value = cjsonPrimitive.getAsUuid();
            buffer.putVarUInt32(ctag.getValue());
            buffer.putUuid(value);
        }
    }

    private void encodeObject(CjsonObject cjsonObject, int ctagName) {
        Ctag ctag = new Ctag(Ctag.OBJECT, ctagName, 0);
        buffer.putVarUInt32(ctag.getValue());
        for (Map.Entry<String, CjsonElement> entry : cjsonObject.entries()) {
            int fieldName = ctagMatcher.getIndex(entry.getKey());
            encodeElement(entry.getValue(), fieldName);
        }
        Ctag endTag = new Ctag(Ctag.END, 0, 0);
        buffer.putVarUInt32(endTag.getValue());
    }

}
