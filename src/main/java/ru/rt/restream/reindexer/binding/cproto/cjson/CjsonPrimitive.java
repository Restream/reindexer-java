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

/**
 * A class representing a Cjson primitive value.
 */
public class CjsonPrimitive extends CjsonElement {

    private final Object value;

    public CjsonPrimitive(String value) {
        this.value = value;
    }

    public CjsonPrimitive(Long value) {
        this.value = value;
    }

    public CjsonPrimitive(Boolean value) {
        this.value = value;
    }

    public CjsonPrimitive(Double value) {
        this.value = value;
    }

    @Override
    public Boolean getAsBoolean() {
        if (value instanceof Long) {
            return (Long) value == 1L;
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        } else if (value instanceof Double) {
            return (Double) value == 1.0D;
        } else if (value instanceof Boolean) {
            return (Boolean) value;
        } else {
            throw new IllegalStateException(String.format("Unexpected value type: %s", value.getClass().getName()));
        }
    }

    @Override
    public String getAsString() {
        return String.valueOf(value);
    }

    @Override
    public Double getAsDouble() {
        if (value instanceof Long) {
            return ((Long) value).doubleValue();
        } else if (value instanceof String) {
            return Double.parseDouble((String) value);
        } else if (value instanceof Double) {
            return (Double) value;
        } else if (value instanceof Boolean) {
            return (Boolean) value ? 1.0D : 0D;
        } else {
            throw new IllegalStateException(String.format("Unexpected value type: %s", value.getClass().getName()));
        }
    }

    @Override
    public Float getAsFloat() {
        if (value instanceof Long) {
            return ((Long) value).floatValue();
        } else if (value instanceof String) {
            return Float.parseFloat((String) value);
        } else if (value instanceof Float) {
            return (Float) value;
        } else if (value instanceof Boolean) {
            return (Boolean) value ? 1.0f : 0f;
        } else {
            throw new IllegalStateException(String.format("Unexpected value type: %s", value.getClass().getName()));
        }
    }

    @Override
    public Long getAsLong() {
        if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof String) {
            return Long.parseLong((String) value);
        } else if (value instanceof Double) {
            return ((Double) value).longValue();
        } else if (value instanceof Boolean) {
            return (Boolean) value ? 1L : 0L;
        } else {
            throw new IllegalStateException(String.format("Unexpected value type: %s", value.getClass().getName()));
        }
    }

    @Override
    public Integer getAsInteger() {
        if (value instanceof Long) {
            return ((Long) value).intValue();
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        } else if (value instanceof Double) {
            return ((Double) value).intValue();
        } else if (value instanceof Boolean) {
            return (Boolean) value ? 1 : 0;
        } else {
            throw new IllegalStateException(String.format("Unexpected value type: %s", value.getClass().getName()));
        }
    }

    @Override
    public Byte getAsByte() {
        if (value instanceof Long) {
            return ((Long) value).byteValue();
        } else if (value instanceof String) {
            return Byte.parseByte((String) value);
        } else if (value instanceof Double) {
            return ((Double) value).byteValue();
        } else if (value instanceof Boolean) {
            return (byte) ((Boolean) value ? 1 : 0);
        } else {
            throw new IllegalStateException(String.format("Unexpected value type: %s", value.getClass().getName()));
        }
    }

    @Override
    public Short getAsShort() {
        if (value instanceof Long) {
            return ((Long) value).shortValue();
        } else if (value instanceof String) {
            return Short.parseShort((String) value);
        } else if (value instanceof Double) {
            return ((Double) value).shortValue();
        } else if (value instanceof Boolean) {
            return (short) ((Boolean) value ? 1 : 0);
        } else {
            throw new IllegalStateException(String.format("Unexpected value type: %s", value.getClass().getName()));
        }
    }

    /**
     * Check that value is an integral type
     *
     * @return true if value is an integral type
     */
    public boolean isIntegral() {
        return value instanceof Long;
    }

    /**
     * Check that value is a double type
     *
     * @return true if value is an double type
     */
    public boolean isDouble() {
        return value instanceof Double;
    }

    /**
     * Check that value is a String type
     *
     * @return true if value is an String type
     */
    public boolean isString() {
        return value instanceof String;
    }

    /**
     * Check that value is a boolean type
     *
     * @return true if value is an boolean type
     */
    public boolean isBoolean() {
        return value instanceof Boolean;
    }

    /**
     * Get value.
     *
     * @return value
     */
    public Object getValue() {
        return value;
    }
}
