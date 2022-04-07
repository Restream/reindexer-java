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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.rt.restream.category.CprotoTest;
import ru.rt.restream.reindexer.Namespace;
import ru.rt.restream.reindexer.NamespaceOptions;
import ru.rt.restream.reindexer.ReindexerIndex;
import ru.rt.restream.reindexer.ReindexerNamespace;
import ru.rt.restream.reindexer.db.DbBaseTest;
import ru.rt.restream.reindexer.fulltext.FullTextConfig;
import ru.rt.restream.reindexer.fulltext.Synonym;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static ru.rt.restream.reindexer.IndexType.TEXT;
import static ru.rt.restream.reindexer.Query.Condition.EQ;

@CprotoTest
public class FullTextTest extends DbBaseTest {

    private Namespace<ItemWithFullTextAnnotation> fullTextNs;
    private Namespace<ItemWithoutFullTextAnnotation> defaultFullTextNs;

    @BeforeEach
    public void setUp() {
        String[] jsonItems = {
                "{\"id\":1,\"name\":\"Processor\",\"description\":\"Processor is the part of a computer.\"}",
                "{\"id\":2,\"name\":\"Chopper\",\"description\":\"Chopper is a food processor of a smaller size.\"}",
                "{\"id\":3,\"name\":\"CPU\",\"description\":\"CPU is the part of a computer.\"}"
        };

        fullTextNs = db.openNamespace("fullTextItems",
                NamespaceOptions.defaultOptions(),
                ItemWithFullTextAnnotation.class);
        defaultFullTextNs = db.openNamespace("defaultFullTextItems",
                NamespaceOptions.defaultOptions(),
                ItemWithoutFullTextAnnotation.class);

        for (String jsonItem : jsonItems) {
            fullTextNs.insert(jsonItem);
            defaultFullTextNs.insert(jsonItem);
        }
    }

    @Test
    public void testClassWithFullTextAnnotationHasFullTextConfig() {
        ReindexerIndex fullTextIndex = getIndexByName(fullTextNs, "description");
        FullTextConfig config = fullTextIndex.getFullTextConfig();
        assertThat(config, notNullValue());
        assertThat(config.getSynonyms(), contains(new Synonym(Arrays.asList("cpu"), Arrays.asList("processor"))));

        ReindexerIndex defaultFullTextIndex = getIndexByName(defaultFullTextNs, "description");
        assertThat(defaultFullTextIndex.getFullTextConfig(), nullValue());
    }

    @Test
    public void testDefaultFullTextSearch() {
        List<ItemWithoutFullTextAnnotation> items = defaultFullTextNs.query()
                .where("description", EQ, "cpu")
                .toList();
        assertThat(items.size(), is(1));
        assertThat(items.get(0).name, is("CPU"));
    }

    @Test
    public void testFullTextSearchSynonyms() {
        List<ItemWithFullTextAnnotation> items = fullTextNs.query()
                .where("description", EQ, "cpu")
                .toList();
        assertThat(items.size(), is(3));
    }

    @Test
    public void testFullTextSearchPatterns() {
        List<ItemWithFullTextAnnotation> items = fullTextNs.query()
                .where("description", EQ, "cpu -food")
                .toList();
        assertThat(items.size(), is(2));
        assertThat(items.get(0).name, is("CPU"));
        assertThat(items.get(1).name, is("Processor"));
    }

    private ReindexerIndex getIndexByName(Namespace<?> ns, String name) {
        return ((ReindexerNamespace<?>) ns).getIndexes().stream()
                .filter(i -> i.getName().equals(name))
                .findFirst()
                .orElse(null);
    }

    public static class ItemWithFullTextAnnotation {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "name")
        private String name;

        @Reindex(name = "description", type = TEXT)
        @FullText(synonyms = @FullText.Synonym(tokens = {"cpu"}, alternatives = {"processor"})
        )
        private String description;

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }
    }

    public static class ItemWithoutFullTextAnnotation {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "name")
        private String name;

        @Reindex(name = "description", type = TEXT)
        private String description;

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }
    }

}
