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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TimeUnitTest {

    @ParameterizedTest
    @CsvSource({
            "nsec,NANOS",
            "usec,MICROS",
            "msec,MILLIS",
            "sec,SECONDS"
    })
    void fromNameWhenMatchesThenReturnsTimeUnit(String name, TimeUnit timeUnit) {
        assertThat(TimeUnit.fromName(name), is(timeUnit));
    }

    @Test
    void fromNameWhenNameNullThenException() {
        NullPointerException thrown = assertThrows(
                NullPointerException.class,
                () -> TimeUnit.fromName(null)
        );
        assertThat(thrown.getMessage(), is("name cannot be null"));
    }

    @Test
    void fromNameWhenDoesNotMatchThenException() {
        IllegalArgumentException thrown = assertThrows(
                IllegalArgumentException.class,
                () -> TimeUnit.fromName("unknown")
        );
        assertThat(thrown.getMessage(), is("Unsupported TimeUnit name: unknown"));
    }

}
