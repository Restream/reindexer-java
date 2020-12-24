/**
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
import ru.rt.restream.reindexer.binding.cproto.cjson.*;

/**
 * Decodes a Cjson byte data to a CjsonElement.
 */
public class CjsonDecoder {

    private final PayloadType payloadType;
    private final ByteBuffer buffer;

    public CjsonDecoder(PayloadType payloadType, ByteBuffer buffer) {
        this.payloadType = payloadType;
        this.buffer = buffer;
    }

    /**
     * This method decodes data into its equivalent representation as a tree of {@link CjsonElement}s.
     */
    public CjsonElement decode() {
        Ctag ctag = new Ctag(buffer.getVarUInt());
        if (ctag.type() == Ctag.OBJECT) {
            return readCjsonObject();
        } else if (ctag.type() == Ctag.ARRAY) {
            return readCjsonArray();
        } else {
            return readCjsonPrimitive(ctag.type());
        }
    }

    private CjsonElement decode(int ctagType) {
        if (ctagType == Ctag.OBJECT) {
            return readCjsonObject();
        } else if (ctagType == Ctag.ARRAY) {
            return readCjsonArray();
        } else if (ctagType == Ctag.BOOL) {
            return readCjsonPrimitive(ctagType);
        } else if (ctagType == Ctag.DOUBLE) {
            return readCjsonPrimitive(ctagType);
        } else if (ctagType == Ctag.VARINT) {
            return readCjsonPrimitive(ctagType);
        } else if (ctagType == Ctag.STRING) {
            return readCjsonPrimitive(ctagType);
        } else if (ctagType == Ctag.NULL) {
            return CjsonNull.INSTANCE;
        } else {
            throw new IllegalStateException("Unexpected ctag type");
        }
    }

    private CjsonElement readCjsonObject() {
        CjsonObject cjsonObject = new CjsonObject();
        Ctag ctag = new Ctag(buffer.getVarUInt());
        while (ctag.type() != Ctag.END) {
            cjsonObject.add(payloadType.tagToName(ctag.name()), decode(ctag.type()));
            ctag = new Ctag(buffer.getVarUInt());
        }
        return cjsonObject;
    }

    private CjsonElement readCjsonArray() {
        CarrayTag carrayTag = new CarrayTag(buffer.getUInt32());
        int count = carrayTag.count();
        int ctagType = carrayTag.tag();
        CjsonArray cjsonArray = new CjsonArray();
        for (int i = 0; i < count; i++) {
            if (ctagType == Ctag.OBJECT) {
                //readObjectTag
                buffer.getVarUInt();
            }
            cjsonArray.add(decode(ctagType));
        }
        return cjsonArray;
    }

    private CjsonElement readCjsonPrimitive(int ctagType) {
        switch (ctagType) {
            case Ctag.VARINT:
                return new CjsonPrimitive(buffer.getVarInt());
            case Ctag.BOOL:
                return new CjsonPrimitive(buffer.getVarUInt() == 1L);
            case Ctag.DOUBLE:
                return new CjsonPrimitive(buffer.getDouble());
            case Ctag.STRING:
                return new CjsonPrimitive(buffer.getVString());
            default:
                throw new IllegalStateException("Not a cjson primitive type");

        }
    }
}
