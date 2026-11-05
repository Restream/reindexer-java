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
import ru.rt.restream.reindexer.TimeUnit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies that the value for the annotated field should be set to the current timestamp using {@code now()} function.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CurrentTimestamp {

    /**
     * A time unit to use in {@code now()} function. Default is {@link TimeUnit#SECONDS}.
     *
     * @return the time unit to use in {@code now()} function, defaults to {@link TimeUnit#SECONDS}
     */
    TimeUnit unit() default TimeUnit.SECONDS;

    /**
     * A precept apply strategy. Default is {@link PreceptApplyStrategy#ALWAYS}.
     *
     * @return the precept apply strategy to use, defaults to {@link PreceptApplyStrategy#ALWAYS}
     */
    PreceptApplyStrategy strategy() default PreceptApplyStrategy.ALWAYS;

}
