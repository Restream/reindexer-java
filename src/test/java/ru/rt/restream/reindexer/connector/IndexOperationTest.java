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
package ru.rt.restream.reindexer.connector;

import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.Test;
import ru.rt.restream.reindexer.CollateMode;
import ru.rt.restream.reindexer.FieldType;
import ru.rt.restream.reindexer.IndexType;
import ru.rt.restream.reindexer.Namespace;
import ru.rt.restream.reindexer.NamespaceOptions;
import ru.rt.restream.reindexer.ReindexerIndex;
import ru.rt.restream.reindexer.annotations.Json;
import ru.rt.restream.reindexer.annotations.Reindex;
import ru.rt.restream.reindexer.db.DbBaseTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.is;

/**
 * Base index operation Test.
 */
public abstract class IndexOperationTest extends DbBaseTest {

    @Test
    public void testOriginalIndexes() {
        String nameSpaceName = "items";
        createNamespaceWithRecords(nameSpaceName, 10);

        List<IndexResponse> indexes = getNameSpaceIndexes(nameSpaceName);

        assertThat(indexes.size(), is(2));
        assertThat(indexes.get(0).getName(), is("description1"));
        assertThat(indexes.get(1).getName(), is("id"));
    }

    @Test
    public void testDropIndex() {
        String nameSpaceName = "drop_index_field_items";
        createNamespaceWithRecords(nameSpaceName, 10);

        db.dropIndex(nameSpaceName, "description1");
        List<IndexResponse> indexes = getNameSpaceIndexes(nameSpaceName);

        assertThat(indexes.size(), is(1));
        assertThat(indexes.get(0).getName(), is("id"));
    }

    @Test
    public void testAddIndex() {
        String nameSpaceName = "added_index_field_items";
        createNamespaceWithRecords(nameSpaceName, 10);
        ReindexerIndex addedIndex = createIndex("description2", "description2", IndexType.HASH, FieldType.STRING);

        db.addIndex(nameSpaceName, addedIndex);
        List<IndexResponse> indexes = getNameSpaceIndexes(nameSpaceName);

        assertThat(indexes.size(), is(3));
        assertThat(indexes.get(0).getName(), is("description1"));
        assertThat(indexes.get(1).getName(), is("description2"));
        assertThat(indexes.get(2).getName(), is("id"));

        assertThat(indexes.get(1).getJsonPaths().get(0), is("description2"));
        assertThat(indexes.get(1).getIndexType(), equalToIgnoringCase("hash"));
        assertThat(indexes.get(1).getFieldType(), equalToIgnoringCase("string"));

    }

    @Test
    public void testUpdateIndex() {
        String nameSpaceName = "updated_index_field_items";
        createNamespaceWithRecords(nameSpaceName, 10);

        List<IndexResponse> beforeUpdate = getNameSpaceIndexes(nameSpaceName);
        // update index with name 'description1': target on field 'description2' and change index type
        ReindexerIndex updIndex = createIndex("description1", "description2", IndexType.TEXT, FieldType.STRING);
        db.updateIndex(nameSpaceName, updIndex);
        List<IndexResponse> afterUpdated = getNameSpaceIndexes(nameSpaceName);

        assertThat(beforeUpdate.size(), is(2));
        assertThat(beforeUpdate.get(1).getName(), is("id"));
        assertThat(beforeUpdate.get(0).getName(), is("description1"));
        assertThat(beforeUpdate.get(0).getJsonPaths().get(0), is("description1"));
        assertThat(beforeUpdate.get(0).getIndexType(), equalToIgnoringCase("hash"));
        assertThat(beforeUpdate.get(0).getFieldType(), equalToIgnoringCase("string"));

        assertThat(afterUpdated.size(), is(2));
        assertThat(afterUpdated.get(1).getName(), is("id"));
        assertThat(afterUpdated.get(0).getName(), is("description1"));
        assertThat(afterUpdated.get(0).getJsonPaths().get(0), is("description2"));
        assertThat(afterUpdated.get(0).getIndexType(), equalToIgnoringCase("text"));
        assertThat(afterUpdated.get(0).getFieldType(), equalToIgnoringCase("string"));
    }

    @Test
    public void testCollateMode() {
        String nameSpaceName = "collate_mode_items";
        Namespace<CollateItem> namespace = db.openNamespace(nameSpaceName, NamespaceOptions.defaultOptions(),
                CollateItem.class);

        List<IndexResponse> indexes = getNameSpaceIndexes(nameSpaceName);
        assertThat(indexes.size(), is(6));
        assertThat(indexNamed(indexes, "id").getCollateMode(), is("none"));
        assertThat(indexNamed(indexes, "plain").getCollateMode(), is("none"));
        assertThat(indexNamed(indexes, "ascii_name").getCollateMode(), is("ascii"));
        assertThat(indexNamed(indexes, "utf8_name").getCollateMode(), is("utf8"));
        assertThat(indexNamed(indexes, "numeric_name").getCollateMode(), is("numeric"));
        assertThat(indexNamed(indexes, "custom_name").getCollateMode(), is("custom"));
        assertThat(indexNamed(indexes, "custom_name").getSortOrderLetters(), is("cba"));
        assertThat(indexNamed(indexes, "ascii_name").getSortOrderLetters(), is(""));

        namespace.insert(collateItem(1, "B", "B", "Б", "10", "a"));
        namespace.insert(collateItem(2, "a", "a", "Я", "2", "b"));
        namespace.insert(collateItem(3, "C", "C", "а", "1", "c"));

        assertThat(sortedBy(namespace, "plain", CollateItem::getPlain), is(Arrays.asList("B", "C", "a")));
        assertThat(sortedBy(namespace, "ascii_name", CollateItem::getAsciiName), is(Arrays.asList("a", "B", "C")));
        assertThat(sortedBy(namespace, "utf8_name", CollateItem::getUtf8Name), is(Arrays.asList("а", "Б", "Я")));
        assertThat(sortedBy(namespace, "numeric_name", CollateItem::getNumericName), is(Arrays.asList("1", "2", "10")));
        assertThat(sortedBy(namespace, "custom_name", CollateItem::getCustomName), is(Arrays.asList("c", "b", "a")));
    }

    private static IndexResponse indexNamed(List<IndexResponse> indexes, String name) {
        for (IndexResponse index : indexes) {
            if (name.equals(index.getName())) {
                return index;
            }
        }
        throw new AssertionError("Index not found: " + name);
    }

    private static List<String> sortedBy(Namespace<CollateItem> namespace, String index,
                                          Function<CollateItem, String> field) {
        List<String> values = new ArrayList<>();
        Iterator<CollateItem> iterator = namespace.query().sort(index, false).execute();
        while (iterator.hasNext()) {
            values.add(field.apply(iterator.next()));
        }
        return values;
    }

    private static CollateItem collateItem(int id, String plain, String asciiName, String utf8Name,
                                            String numericName, String customName) {
        CollateItem item = new CollateItem();
        item.setId(id);
        item.setPlain(plain);
        item.setAsciiName(asciiName);
        item.setUtf8Name(utf8Name);
        item.setNumericName(numericName);
        item.setCustomName(customName);
        return item;
    }

    private ReindexerIndex createIndex(String name, String jsonPath, IndexType indexType, FieldType fieldType) {
        return ReindexerIndex.builder()
                .name(name)
                .jsonPaths(Collections.singletonList(jsonPath))
                .indexType(indexType)
                .fieldType(fieldType)
                .collateMode(CollateMode.NONE)
                .build();
    }

    private void createNamespaceWithRecords(String nameSpaceName, int recordsCnt) {
        Namespace<Item> itemNamespace = db.openNamespace(nameSpaceName, NamespaceOptions.defaultOptions(), Item.class);
        for (int i = 0; i < recordsCnt; i++) {
            Item item = new Item();
            item.setId(i);
            item.setDescription1("IndexedDescription" + i);
            item.setDescription2("NotIndexedDescription" + i);
            itemNamespace.insert(item);
        }
    }

    private List<IndexResponse> getNameSpaceIndexes(String nameSpaceName) {
        Namespace<NamespaceResponse> serviceNamespace = db.openNamespace("#namespaces", NamespaceOptions.defaultOptions(), NamespaceResponse.class);
        String query = String.format("select * from #namespaces where name = '%s'", nameSpaceName);
        Iterator<NamespaceResponse> iterator = serviceNamespace.execSql(query);
        if (iterator.hasNext()) {
            List<IndexResponse> indexes = iterator.next().getIndexes();
            indexes.sort(Comparator.comparing(IndexResponse::getName));
            return indexes;
        }
        return Collections.emptyList();
    }

    @Getter
    @Setter
    public static class Item {
        @Reindex(name = "id", isPrimaryKey = true)
        private int id;
        @Reindex(name = "description1", type = IndexType.HASH)
        private String description1;
        private String description2;
    }

    @Getter
    @Setter
    public static class CollateItem {
        @Reindex(name = "id", isPrimaryKey = true)
        private int id;
        @Reindex(name = "plain", type = IndexType.TREE)
        private String plain;
        @Reindex(name = "ascii_name", type = IndexType.TREE, collate = "ascii")
        private String asciiName;
        @Reindex(name = "utf8_name", type = IndexType.TREE, collate = "utf8")
        private String utf8Name;
        @Reindex(name = "numeric_name", type = IndexType.TREE, collate = "numeric")
        private String numericName;
        @Reindex(name = "custom_name", type = IndexType.TREE, collate = "cba")
        private String customName;
    }

    @Getter
    @Setter
    public static class NamespaceResponse {
        private String name;
        private StorageResponse storage;
        private List<IndexResponse> indexes;
    }

    @Getter
    @Setter
    public static class StorageResponse {
        private boolean enabled;
    }

    @Getter
    @Setter
    public static class IndexResponse {
        private String name;
        @Json("json_paths")
        private List<String> jsonPaths;
        @Json("field_type")
        private String fieldType;
        @Json("index_type")
        private String indexType;
        @Json("is_pk")
        private boolean isPk;
        @Json("is_array")
        private boolean isArray;
        @Json("is_dense")
        private boolean isDense;
        @Json("is_sparse")
        private boolean isSparse;
        @Json("is_linear")
        private boolean isLinear;
        @Json("is_appendable")
        private boolean isAppendable;
        @Json("is_simple_tag")
        private boolean isSimpleTag;
        @Json("collate_mode")
        private String collateMode;
        @Json("sort_order_letters")
        private String sortOrderLetters;
    }
}
