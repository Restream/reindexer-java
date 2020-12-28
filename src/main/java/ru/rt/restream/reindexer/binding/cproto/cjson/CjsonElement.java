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
 * A class representing an element of Cjson. It could either be a {@link CjsonObject}, a
 * {@link CjsonArray}, a {@link CjsonPrimitive} or a {@link CjsonNull}.
 */
public abstract class CjsonElement {

    /**
     * Provides check for verifying if this element is an array or not.
     *
     * @return true if this element is of type {@link CjsonArray}, false otherwise.
     */
    public boolean isArray() {
        return this instanceof CjsonArray;
    }

    /**
     * Provides check for verifying if this element is a Json object or not.
     *
     * @return true if this element is of type {@link CjsonObject}, false otherwise.
     */
    public boolean isObject() {
        return this instanceof CjsonObject;
    }

    /**
     * Provides check for verifying if this element is a primitive or not.
     *
     * @return true if this element is of type {@link CjsonPrimitive}, false otherwise.
     */
    public boolean isPrimitive() {
        return this instanceof CjsonPrimitive;
    }

    /**
     * Provides check for verifying if this element represents a null value or not.
     *
     * @return true if this element is of type {@link CjsonNull}, false otherwise.
     */
    public boolean isNull() {
        return this instanceof CjsonNull;
    }

    /**
     * Convenience method to get this element as a {@link CjsonObject}. If the element is of some
     * other type, a {@link IllegalStateException} will result. Hence it is best to use this method
     * after ensuring that this element is of the desired type by calling {@link #isObject()}
     * first.
     *
     * @return get this element as a {@link CjsonObject}.
     * @throws IllegalStateException if the element is of another type.
     */
    public CjsonObject getAsCjsonObject() {
        if (isObject()) {
            return (CjsonObject) this;
        }
        throw new IllegalStateException("Not a Cjson Object: " + this);
    }

    /**
     * convenience method to get this element as a {@link CjsonArray}. If the element is of some
     * other type, a {@link IllegalStateException} will result. Hence it is best to use this method
     * after ensuring that this element is of the desired type by calling {@link #isArray()}
     * first.
     *
     * @return get this element as a {@link CjsonArray}.
     * @throws IllegalStateException if the element is of another type.
     */
    public CjsonArray getAsCjsonArray() {
        if (isArray()) {
            return (CjsonArray) this;
        }
        throw new IllegalStateException("Not a Cjson Array: " + this);
    }

    /**
     * convenience method to get this element as a {@link CjsonPrimitive}. If the element is of some
     * other type, a {@link IllegalStateException} will result. Hence it is best to use this method
     * after ensuring that this element is of the desired type by calling {@link #isPrimitive()} ()}
     * first.
     *
     * @return get this element as a {@link CjsonPrimitive}.
     * @throws IllegalStateException if the element is of another type.
     */
    public CjsonPrimitive getAsCjsonPrimitive() {
        if (isPrimitive()) {
            return (CjsonPrimitive) this;
        }
        throw new IllegalStateException("Not a Cjson Primitive: " + this);
    }

    /**
     * Convenience method to get this element as a boolean value.
     *
     * @return get this element as a Boolean value.
     */
    public Boolean getAsBoolean() {
        throw new UnsupportedOperationException(getClass().getSimpleName());
    }

    /**
     * Convenience method to get this element as a String value.
     *
     * @return get this element as a String value.
     */
    public String getAsString() {
        throw new UnsupportedOperationException(getClass().getSimpleName());
    }

    /**
     * Convenience method to get this element as a Double value.
     *
     * @return get this element as a Double value.
     */
    public Double getAsDouble() {
        throw new UnsupportedOperationException(getClass().getSimpleName());
    }

    /**
     * Convenience method to get this element as a Float value.
     *
     * @return get this element as a Float value.
     */
    public Float getAsFloat() {
        throw new UnsupportedOperationException(getClass().getSimpleName());
    }

    /**
     * Convenience method to get this element as a Long value.
     *
     * @return get this element as a Long value.
     */
    public Long getAsLong() {
        throw new UnsupportedOperationException(getClass().getSimpleName());
    }

    /**
     * Convenience method to get this element as a Integer value.
     *
     * @return get this element as a Integer value.
     */
    public Integer getAsInteger() {
        throw new UnsupportedOperationException(getClass().getSimpleName());
    }

    /**
     * Convenience method to get this element as a Byte value.
     *
     * @return get this element as a Byte value.
     */
    public Byte getAsByte() {
        throw new UnsupportedOperationException(getClass().getSimpleName());
    }

    /**
     * Convenience method to get this element as a Short value.
     *
     * @return get this element as a Short value.
     */
    public Short getAsShort() {
        throw new UnsupportedOperationException(getClass().getSimpleName());
    }

}
