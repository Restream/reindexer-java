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
package ru.rt.restream.reindexer.annotations;

import ru.rt.restream.reindexer.convert.FieldConverter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies how fields are converted between the Reindexer database type
 * and the one used within the POJO representation.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Convert {

    /**
     * Specifies a {@link FieldConverter} implementation to be used for converting
     * fields between Reindexer database type and the one used within the POJO representation.
     * @return the {@link FieldConverter} implementation to use
     */
    Class<? extends FieldConverter> converterClass() default FieldConverter.class;

    /**
     * Specifies whether conversion should be disabled for the given field,
     * useful in case of global converter should not be applied for specific fields.
     * Defaults to {@literal false}.
     * @return true, if conversion should be disabled for the given field, defaults to {@literal false}
     */
    boolean disableConversion() default false;
}
