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
package ru.rt.restream.reindexer.convert;

/**
 * A {@link FieldConverter} registry interface that allows to configure
 * a custom {@link FieldConverter} implementations.
 */
public interface FieldConverterRegistry {
    
    /**
     * Registers a {@link FieldConverter} for the given Item class and field name.
     * This method allows overriding an existing mapped converter for the given field.
     * 
     * @param itemClass the Item class to use
     * @param fieldName the field name to use
     * @param converter the {@link FieldConverter} to use
     */
    void registerFieldConverter(Class<?> itemClass, String fieldName, FieldConverter<?, ?> converter);

    /**
     * Registers a global {@link FieldConverter}.
     * This method allows overriding an existing global converter.
     *
     * @param converter the {@link FieldConverter} to use
     */
    void registerGlobalConverter(FieldConverter<?, ?> converter);
}
