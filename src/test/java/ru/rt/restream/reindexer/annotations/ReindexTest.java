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

import org.junit.jupiter.api.Test;
import ru.rt.restream.category.BuiltinTest;
import ru.rt.restream.reindexer.NamespaceOptions;
import ru.rt.restream.reindexer.db.DbBaseTest;
import ru.rt.restream.reindexer.exceptions.IndexConflictException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.rt.restream.reindexer.IndexType.TEXT;

@BuiltinTest
public class ReindexTest extends DbBaseTest {

    @Test
    public void testThrownExceptionWhenNonUniqueIndexes() {
        IndexConflictException thrown = assertThrows(
                IndexConflictException.class,
                () -> db.openNamespace("someItems",
                        NamespaceOptions.defaultOptions(),
                        ItemWithNonUniqueIndexes.class),
                "Expected IndexConflictException() to throw, but it didn't"
        );
        assertTrue(thrown.getMessage().startsWith("Non-unique index name in class"));
    }

    static class ItemWithNonUniqueIndexes {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "name")
        private String name;

        @Reindex(name = "name", type = TEXT)
        private String description;
    }

}
