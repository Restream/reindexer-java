/*
 * Copyright 2020-present Restream
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

import ru.rt.restream.reindexer.PreceptApplyStrategy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Sequence of integer, useful for unique ID generation.
 * When used on primary-key index, consider using {@link PreceptApplyStrategy#WHEN_NULL} to generate ID value only once.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Serial {

    /**
     * A precept apply strategy. Default is {@link PreceptApplyStrategy#ALWAYS}.
     *
     * @return the precept apply strategy to use, defaults to {@link PreceptApplyStrategy#ALWAYS}
     */
    PreceptApplyStrategy strategy() default PreceptApplyStrategy.ALWAYS;

}
