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

class Ctag {

    static final int VARINT = 0;

    static final int DOUBLE = 1;

    static final int STRING = 2;

    static final int BOOL = 3;

    static final int NULL = 4;

    static final int ARRAY = 5;

    static final int OBJECT = 6;

    static final int END = 7;

    static final int UUID = 8;

    static final int FLOAT = 9;

    /**
     * |31     29|28        25|24     15|14     3|2     0|
     * |  TYPE1  |  RESERVED  |  FIELD  |  NAME  | TYPE0 |
     * TYPE0 and TYPE1 - 6bit  type: one of TAG_XXXX
     * NAME - 12bit index+1 of field name in tagsMatcher (0 if no name)
     * FIELD - 10bit index+1 of field in reindexer Payload (0 if no field)
     */
    static final int TYPE0_BITS = 3;
    static final int NAME_BITS = 12;
    static final int FIELD_BITS = 10;
    static final int TYPE1_BITS = 3;

    static final int NAME_SHIFT = 3;
    static final int FIELD_SHIFT = 15;
    static final int TYPE1_SHIFT = 29 - TYPE0_BITS;

    static final int TYPE0_MASK = (1 << TYPE0_BITS) - 1; // 000111
    static final int TYPE1_MASK = ((1 << TYPE1_BITS) - 1) << TYPE0_BITS; // 111000
    static final int NAME_MASK = (1 << NAME_BITS) - 1;
    static final int FIELD_MASK = (1 << FIELD_BITS) - 1;

    private final long value;

    Ctag(long value) {
        this.value = value;
    }

    Ctag(int tagType, int tagName, int tagField) {
        this((tagType & TYPE0_MASK)
                | ((long) (tagType & TYPE1_MASK) << TYPE1_SHIFT)
                | ((tagName & NAME_MASK) << NAME_SHIFT)
                | ((tagField & FIELD_MASK) << FIELD_SHIFT));
    }

    public int name() {
        return (int) (value >> NAME_SHIFT & NAME_MASK);
    }

    public int type() {
        return (int) ((value & TYPE0_MASK)
                | (value >> TYPE1_SHIFT & TYPE1_MASK));
    }

    public int field() {
        return (int) (value >> FIELD_SHIFT & FIELD_MASK);
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
        } else if (tagType == FLOAT) {
            return "<float>";
        } else if (tagType == NULL) {
            return "<null>";
        } else if (tagType == UUID) {
            return "<uuid>";
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
