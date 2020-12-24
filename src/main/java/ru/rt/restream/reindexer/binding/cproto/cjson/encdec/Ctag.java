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

class Ctag {

    static final int VARINT = 0;

    static final int DOUBLE = 1;

    static final int STRING = 2;

    static final int BOOL = 3;

    static final int NULL = 4;

    static final int ARRAY = 5;

    static final int OBJECT = 6;

    static final int END = 7;

    static final int TYPE_BITS = 3;

    static final int NAME_BITS = 12;

    private final long value;

    Ctag(long value) {
        this.value = value;
    }

    Ctag(int tagType, int tagName, int tagField) {
        this((tagType | (tagName << TYPE_BITS) | (tagField << (NAME_BITS + TYPE_BITS))));
    }

    public int name() {
        return (int) ((value >> TYPE_BITS) & ((1 << NAME_BITS) - 1));
    }

    public int type() {
        return (int) (value & ((1 << TYPE_BITS) - 1));
    }

    public int field() {
        return (int) ((value >> (TYPE_BITS + NAME_BITS)) - 1);
    }

    String tagTypeName(long tagType) {
        if (tagType == VARINT) {
            return "<varint>";
        } else if (tagType == OBJECT) {
            return "<object>";
        } else if (tagType == END) {
            return "<end>";
        } else if (tagType == ARRAY) {
            return "<array>";
        } else if (tagType == BOOL) {
            return "<bool>";
        } else if (tagType == STRING) {
            return "<string>";
        } else if (tagType == DOUBLE) {
            return "<double>";
        } else if (tagType == NULL) {
            return "<null>";
        }
        return String.format("<unknown %d>", tagType);
    }

    public long getValue() {
        return value;
    }

    String dump() {
        return String.format("(%s n:%d f:%d)", tagTypeName(value), name(), field());
    }

}
