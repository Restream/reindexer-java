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

package ru.rt.restream.reindexer.fast;

import org.junit.jupiter.api.Test;
import ru.rt.restream.reindexer.ReindexScanner;
import ru.rt.restream.reindexer.ReindexerIndex;
import ru.rt.restream.reindexer.annotations.Reindex;
import ru.rt.restream.reindexer.annotations.ReindexAnnotationScanner;
import ru.rt.restream.reindexer.exceptions.IndexConflictException;

import java.util.List;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.rt.restream.reindexer.IndexType.TEXT;

public class ReindexAnnotationScannerIsAppendableIndexTest {
    private final ReindexScanner scanner = new ReindexAnnotationScanner();

    @Test
    public void testThrownExceptionWhenNonUniqueNonIsAppendableIndexes() {
        IndexConflictException thrown = assertThrows(
                IndexConflictException.class,
                () -> scanner.parseIndexes(ItemWithNonUniqueIndexes.class),
                "Expected IndexConflictException() to throw, but it didn't"
        );
        assertTrue(thrown.getMessage().startsWith("Non-unique index name in class"));
    }

    @Test
    public void testIsAppendableIndexes() {
        List<ReindexerIndex> indexes = scanner.parseIndexes(ItemWithAppendableIndexes.class);
        ReindexerIndex appendable = getIndexByName(indexes, "name");

        assertThat(indexes.size(), is(2));
        assertThat(appendable, notNullValue());
        assertTrue(appendable.isAppendable());
        assertTrue(appendable.isArray());
        assertThat(appendable.getJsonPaths(), hasItems("name", "description"));
    }

    @Test
    public void testDifferentIsAppendableIndexes() {
        IndexConflictException thrown = assertThrows(
                IndexConflictException.class,
                () -> scanner.parseIndexes(ItemWithDifferentAppendableIndexes.class),
                "Expected IndexConflictException() to throw, but it didn't"
        );
        assertTrue(thrown.getMessage().matches("Appendable indexes with name .* must have the same configuration"));
    }

    @Test
    public void testAppendableAndNonAppendableIndexes() {
        IndexConflictException thrown = assertThrows(
                IndexConflictException.class,
                () -> scanner.parseIndexes(ItemWithOnlyOneAppendableIndex.class),
                "Expected IndexConflictException() to throw, but it didn't"
        );
        assertTrue(thrown.getMessage().matches("Multiple indexes with name .* " +
                "but at least one of them is not marked as appendable"));
    }

    private ReindexerIndex getIndexByName(List<ReindexerIndex> indexes, String indexName) {
        for (ReindexerIndex index : indexes) {
            if (Objects.equals(index.getName(), indexName)) {
                return index;
            }
        }
        return null;
    }


    static class ItemWithNonUniqueIndexes {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "name")
        private String name;

        @Reindex(name = "name")
        private String description;
    }

    static class ItemWithAppendableIndexes {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "name", isAppendable = true, type = TEXT)
        private String name;

        @Reindex(name = "name", isAppendable = true, type = TEXT)
        private String description;
    }

    static class ItemWithDifferentAppendableIndexes {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "name", isAppendable = true, type = TEXT, isDense = true)
        private String name;

        @Reindex(name = "name", isAppendable = true, type = TEXT)
        private String description;
    }

    static class ItemWithOnlyOneAppendableIndex {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "name", isAppendable = true, type = TEXT)
        private String name;

        @Reindex(name = "name", type = TEXT)
        private String description;
    }
}
