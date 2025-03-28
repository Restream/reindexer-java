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
import org.junit.jupiter.api.Test;
import ru.rt.restream.reindexer.Namespace;
import ru.rt.restream.reindexer.NamespaceOptions;
import ru.rt.restream.reindexer.Query;
import ru.rt.restream.reindexer.annotations.Reindex;
import ru.rt.restream.reindexer.annotations.Transient;
import ru.rt.restream.reindexer.db.DbBaseTest;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Base Merge test.
 */
public abstract class MergeTest extends DbBaseTest {

    @Test
    public void testMerge() {
        Namespace<Item> itemNamespace = db.openNamespace("items", NamespaceOptions.defaultOptions(), Item.class);

        for (int i = 0; i < 100; i++) {
            itemNamespace.insert(new Item(i, i % 2 == 0, i));
        }

        Query<Item> foo = itemNamespace.query()
                .merge(itemNamespace.query()
                        .where("foo", Query.Condition.EQ, false))
                .where("foo", Query.Condition.EQ, true);
        List<Item> items = foo.toList();
        assertThat(items.size(), is(100));
    }

    @Test
    public void testMergeWithJoins() {
        Namespace<Item> itemNamespace = db.openNamespace("items", NamespaceOptions.defaultOptions(), Item.class);
        Namespace<SubItem> subItemNamespace = db.openNamespace("sub_items", NamespaceOptions.defaultOptions(), SubItem.class);

        for (int i = 0; i < 100; i++) {
            itemNamespace.insert(new Item(i, i % 2 == 0, i));
            subItemNamespace.insert(new SubItem(i));
        }

        Query<Item> foo = itemNamespace.query()
                .join(subItemNamespace.query().on("subItemId", Query.Condition.EQ, "id"),
                        "subItem")
                .merge(itemNamespace.query()
                        .join(subItemNamespace.query().on("subItemId", Query.Condition.EQ, "id"),
                                "subItem")
                        .where("foo", Query.Condition.EQ, false))
                .where("foo", Query.Condition.EQ, true);
        List<Item> items = foo.toList();
        assertThat(items.size(), is(100));
        for (Item item : items) {
            assertThat(item.subItem.id, is(item.subItemId));
        }
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class Item {
        @Reindex(name = "id", isPrimaryKey = true)
        private int id;

        @Reindex(name = "foo")
        private boolean foo;

        private int subItemId;

        @Transient
        private SubItem subItem;

        public Item(int id, boolean foo, int subItemId) {
            this.id = id;
            this.foo = foo;
            this.subItemId = subItemId;
        }

    }

    @Setter
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SubItem {
        @Reindex(name = "id", isPrimaryKey = true)
        private int id;
    }

}
