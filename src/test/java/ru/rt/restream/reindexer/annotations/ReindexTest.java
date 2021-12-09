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
import ru.rt.restream.reindexer.NamespaceOptions;
import ru.rt.restream.reindexer.db.DbBaseTest;
import ru.rt.restream.reindexer.db.DbLocator;
import ru.rt.restream.reindexer.exceptions.IndexConflictException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.rt.restream.reindexer.IndexType.TEXT;

public class ReindexTest extends DbBaseTest {

    @Override
    protected DbLocator.Type getDbType() {
        return DbLocator.Type.BUILTIN;
    }

    @Test
    public void testThrownExceptionWhenNonUniqueIndexes() {
        IndexConflictException thrown = assertThrows(
                IndexConflictException.class,
                () -> db.openNamespace("someItems",
                        NamespaceOptions.defaultOptions(),
                        ItemWithNonUniqueIndexes.class),
                "Expected IndexConflictException() to throw, but it didn't"
        );
        assertTrue(thrown.getMessage().startsWith("Non-unique name index name in class"));
    }

    @Test
    public void testNoExceptionWhen63Indexes() {
        db.openNamespace("someItems63",
                NamespaceOptions.defaultOptions(),
                ItemWith63Indexes.class);

    }

    @Test
    public void testThrownExceptionWhen64Indexes() {
        IndexConflictException thrown = assertThrows(
                IndexConflictException.class,
                () -> db.openNamespace("someItems64",
                        NamespaceOptions.defaultOptions(),
                        ItemWith64Indexes.class),
                "Expected IndexConflictException() to throw, but it didn't"
        );
        assertTrue(thrown.getMessage().startsWith("Too many indexes in the class"));
    }

    static class ItemWithNonUniqueIndexes {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "name")
        private String name;

        @Reindex(name = "name", type = TEXT)
        private String description;
    }

    static class ItemWith63Indexes {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "name0")
        private String name0;
        @Reindex(name = "name1")
        private String name1;
        @Reindex(name = "name2")
        private String name2;
        @Reindex(name = "name3")
        private String name3;
        @Reindex(name = "name4")
        private String name4;
        @Reindex(name = "name5")
        private String name5;
        @Reindex(name = "name6")
        private String name6;
        @Reindex(name = "name7")
        private String name7;
        @Reindex(name = "name8")
        private String name8;
        @Reindex(name = "name9")
        private String name9;

        @Reindex(name = "name10")
        private String name10;
        @Reindex(name = "name11")
        private String name11;
        @Reindex(name = "name12")
        private String name12;
        @Reindex(name = "name13")
        private String name13;
        @Reindex(name = "name14")
        private String name14;
        @Reindex(name = "name15")
        private String name15;
        @Reindex(name = "name16")
        private String name16;
        @Reindex(name = "name17")
        private String name17;
        @Reindex(name = "name18")
        private String name18;
        @Reindex(name = "name19")
        private String name19;

        @Reindex(name = "name20")
        private String name20;
        @Reindex(name = "name21")
        private String name21;
        @Reindex(name = "name22")
        private String name22;
        @Reindex(name = "name23")
        private String name23;
        @Reindex(name = "name24")
        private String name24;
        @Reindex(name = "name25")
        private String name25;
        @Reindex(name = "name26")
        private String name26;
        @Reindex(name = "name27")
        private String name27;
        @Reindex(name = "name28")
        private String name28;
        @Reindex(name = "name29")
        private String name29;

        @Reindex(name = "name30")
        private String name30;
        @Reindex(name = "name31")
        private String name31;
        @Reindex(name = "name32")
        private String name32;
        @Reindex(name = "name33")
        private String name33;
        @Reindex(name = "name34")
        private String name34;
        @Reindex(name = "name35")
        private String name35;
        @Reindex(name = "name36")
        private String name36;
        @Reindex(name = "name37")
        private String name37;
        @Reindex(name = "name38")
        private String name38;
        @Reindex(name = "name39")
        private String name39;

        @Reindex(name = "name40")
        private String name40;
        @Reindex(name = "name41")
        private String name41;
        @Reindex(name = "name42")
        private String name42;
        @Reindex(name = "name43")
        private String name43;
        @Reindex(name = "name44")
        private String name44;
        @Reindex(name = "name45")
        private String name45;
        @Reindex(name = "name46")
        private String name46;
        @Reindex(name = "name47")
        private String name47;
        @Reindex(name = "name48")
        private String name48;
        @Reindex(name = "name49")
        private String name49;

        @Reindex(name = "name50")
        private String name50;
        @Reindex(name = "name51")
        private String name51;
        @Reindex(name = "name52")
        private String name52;
        @Reindex(name = "name53")
        private String name53;
        @Reindex(name = "name54")
        private String name54;
        @Reindex(name = "name55")
        private String name55;
        @Reindex(name = "name56")
        private String name56;
        @Reindex(name = "name57")
        private String name57;
        @Reindex(name = "name58")
        private String name58;
        @Reindex(name = "name59")
        private String name59;
        @Reindex(name = "name60")
        private String name60;
        @Reindex(name = "name61")
        private String name61;
    }

    static class ItemWith64Indexes {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "name0")
        private String name0;
        @Reindex(name = "name1")
        private String name1;
        @Reindex(name = "name2")
        private String name2;
        @Reindex(name = "name3")
        private String name3;
        @Reindex(name = "name4")
        private String name4;
        @Reindex(name = "name5")
        private String name5;
        @Reindex(name = "name6")
        private String name6;
        @Reindex(name = "name7")
        private String name7;
        @Reindex(name = "name8")
        private String name8;
        @Reindex(name = "name9")
        private String name9;

        @Reindex(name = "name10")
        private String name10;
        @Reindex(name = "name11")
        private String name11;
        @Reindex(name = "name12")
        private String name12;
        @Reindex(name = "name13")
        private String name13;
        @Reindex(name = "name14")
        private String name14;
        @Reindex(name = "name15")
        private String name15;
        @Reindex(name = "name16")
        private String name16;
        @Reindex(name = "name17")
        private String name17;
        @Reindex(name = "name18")
        private String name18;
        @Reindex(name = "name19")
        private String name19;

        @Reindex(name = "name20")
        private String name20;
        @Reindex(name = "name21")
        private String name21;
        @Reindex(name = "name22")
        private String name22;
        @Reindex(name = "name23")
        private String name23;
        @Reindex(name = "name24")
        private String name24;
        @Reindex(name = "name25")
        private String name25;
        @Reindex(name = "name26")
        private String name26;
        @Reindex(name = "name27")
        private String name27;
        @Reindex(name = "name28")
        private String name28;
        @Reindex(name = "name29")
        private String name29;

        @Reindex(name = "name30")
        private String name30;
        @Reindex(name = "name31")
        private String name31;
        @Reindex(name = "name32")
        private String name32;
        @Reindex(name = "name33")
        private String name33;
        @Reindex(name = "name34")
        private String name34;
        @Reindex(name = "name35")
        private String name35;
        @Reindex(name = "name36")
        private String name36;
        @Reindex(name = "name37")
        private String name37;
        @Reindex(name = "name38")
        private String name38;
        @Reindex(name = "name39")
        private String name39;

        @Reindex(name = "name40")
        private String name40;
        @Reindex(name = "name41")
        private String name41;
        @Reindex(name = "name42")
        private String name42;
        @Reindex(name = "name43")
        private String name43;
        @Reindex(name = "name44")
        private String name44;
        @Reindex(name = "name45")
        private String name45;
        @Reindex(name = "name46")
        private String name46;
        @Reindex(name = "name47")
        private String name47;
        @Reindex(name = "name48")
        private String name48;
        @Reindex(name = "name49")
        private String name49;

        @Reindex(name = "name50")
        private String name50;
        @Reindex(name = "name51")
        private String name51;
        @Reindex(name = "name52")
        private String name52;
        @Reindex(name = "name53")
        private String name53;
        @Reindex(name = "name54")
        private String name54;
        @Reindex(name = "name55")
        private String name55;
        @Reindex(name = "name56")
        private String name56;
        @Reindex(name = "name57")
        private String name57;
        @Reindex(name = "name58")
        private String name58;
        @Reindex(name = "name59")
        private String name59;
        @Reindex(name = "name60")
        private String name60;
        @Reindex(name = "name61")
        private String name61;
        @Reindex(name = "name62")
        private String name62;
    }

}
