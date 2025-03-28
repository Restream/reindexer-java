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
package ru.rt.restream.reindexer.connector;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.rt.restream.reindexer.Namespace;
import ru.rt.restream.reindexer.NamespaceOptions;
import ru.rt.restream.reindexer.ResultIterator;
import ru.rt.restream.reindexer.annotations.FullText;
import ru.rt.restream.reindexer.annotations.Reindex;
import ru.rt.restream.reindexer.db.DbBaseTest;
import ru.rt.restream.reindexer.util.Pair;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static ru.rt.restream.reindexer.IndexType.TEXT;
import static ru.rt.restream.reindexer.Query.Condition.EQ;
import static ru.rt.restream.reindexer.binding.Consts.EMPTY_RANK;

/**
 * Base float full text rank test.
 */
public abstract class FullTextRankTest extends DbBaseTest {

    private final String namespaceName = "items";
    private Namespace<FullTextItem> fullTextNs;

    @BeforeEach
    public void setUp() {
        fullTextNs = db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), FullTextItem.class);
        String[] jsonItems = {
                "{\"id\":1,\"name\":\"Processor\",\"description\":\"Processor is the part of a computer.\"}",
                "{\"id\":2,\"name\":\"Chopper\",\"description\":\"Chopper is a food processor of a smaller size.\"}",
                "{\"id\":3,\"name\":\"CPU\",\"description\":\"CPU is the part of a computer.\"}"
        };
        for (String jsonItem : jsonItems) {
            fullTextNs.insert(jsonItem);
        }
    }

    @Test
    public void testGetItemsWithRank_isOk() {
        ResultIterator<FullTextItem> iterator = fullTextNs.query()
                .withRank()
                .where("description", EQ, "cpu")
                .execute();

        int count = 0;
        while (iterator.hasNext()) {
            FullTextItem item = iterator.next();
            float rank = iterator.getCurrentRank();
            assertThat(rank, is(not(EMPTY_RANK)));
            count++;
        }

        assertThat(count, is(3));
    }

    @Test
    public void testGetAllItemsWithRank_isOk() {
        Pair<List<FullTextItem>, float[]> result = fullTextNs.query()
                .where("description", EQ, "cpu")
                .executeAllWithRank();
        List<FullTextItem> items = result.getFirst();
        float[] ranks = result.getSecond();

        assertThat(items.size(), is(3));
        assertThat(ranks.length, is(3));
        for (float rank : ranks) {
            assertThat(rank, is(not(EMPTY_RANK)));
        }
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FullTextItem {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "name")
        private String name;

        @Reindex(name = "description", type = TEXT)
        @FullText(synonyms = @FullText.Synonym(tokens = {"cpu"}, alternatives = {"processor"}))
        private String description;
    }

}
