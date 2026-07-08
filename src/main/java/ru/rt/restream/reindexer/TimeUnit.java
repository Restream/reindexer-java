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
package ru.rt.restream.reindexer;

import lombok.Getter;

import java.util.Objects;

/**
 * Represents a time unit to use in date functions.
 */
public enum TimeUnit {

    /**
     * Nanoseconds time unit.
     */
    NANOS("nsec"),

    /**
     * Microseconds time unit.
     */
    MICROS("usec"),

    /**
     * Milliseconds time unit.
     */
    MILLIS("msec"),

    /**
     * Seconds time unit.
     */
    SECONDS("sec"),
    ;

    /**
     * Returns the {@code TimeUnit} for the given {@code name}.
     *
     * @param name the time unit name
     * @return the {@link TimeUnit} to use
     */
    public static TimeUnit fromName(String name) {
        Objects.requireNonNull(name, "name cannot be null");
        for (TimeUnit timeUnit : TimeUnit.values()) {
            if (timeUnit.name.equalsIgnoreCase(name)) {
                return timeUnit;
            }
        }
        throw new IllegalArgumentException("Unsupported TimeUnit name: " + name);
    }

    @Getter
    private final String name;

    TimeUnit(String name) {
        this.name = name;
    }

}
