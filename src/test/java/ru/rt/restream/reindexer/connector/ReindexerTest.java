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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.classic.methods.HttpGet;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.classic.methods.HttpPost;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.HttpClients;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.io.entity.StringEntity;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import ru.rt.restream.reindexer.CloseableIterator;
import ru.rt.restream.reindexer.Configuration;
import ru.rt.restream.reindexer.Reindexer;
import ru.rt.restream.reindexer.Transaction;
import ru.rt.restream.reindexer.annotations.Reindex;
import ru.rt.restream.reindexer.annotations.Serial;
import ru.rt.restream.reindexer.binding.option.NamespaceOptions;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static ru.rt.restream.reindexer.Query.Condition.EQ;
import static ru.rt.restream.reindexer.Query.Condition.RANGE;

@Testcontainers
public class ReindexerTest {

    @Container
    public GenericContainer<?> reindexer = new GenericContainer<>(DockerImageName.parse("reindexer/reindexer:v2.14.1"))
            .withExposedPorts(9088, 6534);

    private Reindexer db;

    private String restApiPort = "9088";
    private String rpcPort = "6534";

    @BeforeEach
    public void setUp() {
        restApiPort = String.valueOf(reindexer.getMappedPort(9088));
        rpcPort = String.valueOf(reindexer.getMappedPort(6534));
        CreateDatabase createDatabase = new CreateDatabase();
        createDatabase.setName("test_items");
        post("/db", createDatabase);

        this.db = Configuration.builder()
                .url("cproto://" + "localhost:" + rpcPort + "/test_items")
                .connectionPoolSize(4)
                .requestTimeout(Duration.ofSeconds(30L))
                .getReindexer();
    }

    @AfterEach
    void tearDown() {
        if (db != null) {
            db.close();
        }
    }

    @Test
    public void testOpenNamespace() {
        String namespaceName = "items";

        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        NamespaceResponse namespaceResponse = get("/db/test_items/namespaces/items", NamespaceResponse.class);
        assertThat(namespaceResponse.name, is(namespaceName));
        assertThat(namespaceResponse.indexes.size(), is(9));
        assertThat(namespaceResponse.storage.enabled, is(true));
        List<NamespaceResponse.IndexResponse> indexes = namespaceResponse.indexes;
        NamespaceResponse.IndexResponse idIdx = indexes.get(0);
        assertThat(idIdx.isPk, is(true));
        assertThat(idIdx.name, is("id"));
        assertThat(idIdx.fieldType, is("int"));
        NamespaceResponse.IndexResponse nameIdx = indexes.get(1);
        assertThat(nameIdx.isPk, is(false));
        assertThat(nameIdx.name, is("name"));
        assertThat(nameIdx.fieldType, is("string"));
    }

    @Test
    public void testInsertItem() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");

        db.insert(namespaceName, testItem);

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .execute();
        assertThat(iterator.hasNext(), is(true));

        TestItem item = iterator.next();
        assertThat(item.id, is(testItem.id));
        assertThat(item.name, is(testItem.name));
        assertThat(item.nonIndex, is(testItem.nonIndex));
    }

    @Test
    public void testUpdateItem() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");

        db.insert(namespaceName, testItem);

        testItem.setName("TestNameUpdated");

        db.update(namespaceName, testItem);

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .execute();
        assertThat(iterator.hasNext(), is(true));

        TestItem item = iterator.next();
        assertThat(item.id, is(testItem.id));
        assertThat(item.name, is(testItem.name));
        assertThat(item.nonIndex, is(testItem.nonIndex));
    }

    @Test
    public void testUpsertItem() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");

        db.upsert(namespaceName, testItem);

        ItemsResponse itemsResponse = get("/db/test_items/namespaces/items/items", ItemsResponse.class);
        assertThat(itemsResponse.totalItems, is(1));
        TestItem responseItem = itemsResponse.items.get(0);
        assertThat(responseItem.name, is(testItem.name));
        assertThat(responseItem.id, is(testItem.id));
    }

    @Test
    public void testDeleteItem() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");

        db.insert(namespaceName, testItem);

        db.delete(namespaceName, testItem);

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .execute();
        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testSelectOneItem() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .execute();

        assertThat(iterator.hasNext(), is(true));

        TestItem next = iterator.next();
        assertThat(next.id, is(77));
        assertThat(next.name, is("TestName77"));
        assertThat(next.value, is("77Value"));

        assertThat(iterator.hasNext(), is(false));

    }

    @Test
    public void testSelectItemByOrCondition() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .or()
                .where("id", EQ, 78)
                .execute();

        assertThat(iterator.hasNext(), is(true));

        TestItem next = iterator.next();
        assertThat(next.id, is(77));
        assertThat(next.name, is("TestName77"));
        assertThat(next.value, is("77Value"));

        assertThat(iterator.hasNext(), is(true));
        next = iterator.next();
        assertThat(next.id, is(78));
        assertThat(next.name, is("TestName78"));
        assertThat(next.value, is("78Value"));

        assertThat(iterator.hasNext(), is(false));

    }

    @Test
    public void testSelectItemsByNotCondition() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        List<TestItem> queryItems = db.query("items", TestItem.class)
                .not()
                .where("id", EQ, 77)
                .toList();

        assertThat(queryItems.size(), is(99));
        assertThat(queryItems.stream().anyMatch(testItem -> testItem.id == 77), is(false));
    }

    @Test
    public void testSelectItemsByConditionsInBracket() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .or()
                .openBracket()
                .where("id", RANGE, 80, 90)
                .where("value", EQ, "85Value")
                .closeBracket()
                .execute();

        assertThat(iterator.hasNext(), is(true));

        TestItem byId = iterator.next();
        assertThat(byId.id, is(77));
        assertThat(byId.name, is("TestName77"));
        assertThat(byId.value, is("77Value"));

        TestItem byRangeAndValue = iterator.next();
        assertThat(byRangeAndValue.id, is(85));
        assertThat(byRangeAndValue.value, is("85Value"));

        assertThat(iterator.hasNext(), is(false));

    }

    @Test
    public void testSelectOneItemByCompositeIndex() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .whereComposite("id+name", EQ, 77, "TestName77")
                .execute();

        assertThat(iterator.hasNext(), is(true));

        TestItem next = iterator.next();
        assertThat(next.id, is(77));
        assertThat(next.name, is("TestName77"));
        assertThat(next.value, is("77Value"));

        assertThat(iterator.hasNext(), is(false));

    }

    @Test
    public void testSelectOneByNestedIndexes() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");

            NestedTest nestedTest = new NestedTest();
            nestedTest.test = i;
            nestedTest.value = "nestedValue" + i;
            testItem.setNestedTest(nestedTest);

            db.upsert(namespaceName, testItem);
        }

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("nestedTest.test", EQ, 77)
                .where("nestedTest.value", EQ, "nestedValue77")
                .execute();

        assertThat(iterator.hasNext(), is(true));

        TestItem next = iterator.next();
        assertThat(next.id, is(77));
        assertThat(next.name, is("TestName77"));
        assertThat(next.value, is("77Value"));

        assertThat(iterator.hasNext(), is(false));

    }

    @Test
    public void testSelectOneByNestedArrayIndex() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");

            NestedTest nestedTest = new NestedTest();
            nestedTest.test = i;
            nestedTest.value = "nestedValue" + i;
            testItem.setNestedTest(nestedTest);

            List<NestedTest> nestedList = new ArrayList<>();
            NestedTest arrayItem = new NestedTest();
            arrayItem.value = "array" + i;
            arrayItem.test = i;
            nestedList.add(arrayItem);
            testItem.setListNested(nestedList);

            db.upsert(namespaceName, testItem);
        }

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("listNested.test", EQ, 77)
                .where("listNested.value", EQ, "array77")
                .execute();

        assertThat(iterator.hasNext(), is(true));

        TestItem next = iterator.next();
        assertThat(next.id, is(77));
        assertThat(next.name, is("TestName77"));
        assertThat(next.value, is("77Value"));

        assertThat(iterator.hasNext(), is(false));

    }

    @Test
    public void testSelectOneByArrayItem() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");

            NestedTest nestedTest = new NestedTest();
            nestedTest.test = i;
            nestedTest.value = "nestedValue" + i;
            testItem.setNestedTest(nestedTest);

            List<Integer> integers = new ArrayList<>();
            integers.add(i);
            testItem.setIntegers(integers);

            db.upsert(namespaceName, testItem);
        }

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("integers", EQ, 77)
                .execute();

        assertThat(iterator.hasNext(), is(true));

        TestItem next = iterator.next();
        assertThat(next.id, is(77));
        assertThat(next.name, is("TestName77"));
        assertThat(next.value, is("77Value"));

        assertThat(iterator.hasNext(), is(false));

    }

    @Test
    public void testSelectOneItemByThreePredicates() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .where("name", EQ, "TestName77")
                .where("value", EQ, "77Value")
                .execute();

        assertThat(iterator.hasNext(), is(true));

        TestItem next = iterator.next();
        assertThat(next.id, is(77));
        assertThat(next.name, is("TestName77"));
        assertThat(next.value, is("77Value"));

        assertThat(iterator.hasNext(), is(false));

    }

    @Test
    public void testSelectOneItemByThreePredicatesWhenOneFieldIsNotMatching() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .where("name", EQ, "TestName77")
                .where("value", EQ, "notEquals")
                .execute();

        assertThat(iterator.hasNext(), is(false));

    }

    @Test
    public void testDeleteOneItem() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .delete();

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .execute();

        assertThat(iterator.hasNext(), is(false));

    }

    @Test
    public void testDeleteListItem() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        db.query("items", TestItem.class)
                .where("id", EQ, 77, 17, 7)
                .delete();

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("id", EQ, 77, 17, 7)
                .execute();

        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testDeleteAllItems() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        db.query("items", TestItem.class)
                .delete();

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .execute();

        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testSelectItemList() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        Set<TestItem> expectedItems = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
            expectedItems.add(testItem);
        }

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .execute();

        while (iterator.hasNext()) {
            TestItem responseItem = iterator.next();
            assertThat(expectedItems.remove(responseItem), is(true));
        }

        assertThat(expectedItems.size(), is(0));
    }

    @Test
    public void testSelectItemWithLimit() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        Set<TestItem> expectedItems = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
            expectedItems.add(testItem);
        }

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .limit(10)
                .fetchCount(1)
                .execute();

        while (iterator.hasNext()) {
            TestItem responseItem = iterator.next();
            assertThat(expectedItems.remove(responseItem), is(true));
        }

        assertThat(expectedItems.size(), is(90));
    }

    @Test
    public void testSelectItemWithOffset() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        Set<TestItem> expectedItems = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
            expectedItems.add(testItem);
        }

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .offset(50)
                .execute();

        while (iterator.hasNext()) {
            TestItem responseItem = iterator.next();
            assertThat(expectedItems.remove(responseItem), is(true));
        }

        assertThat(expectedItems.size(), is(50));
        Integer maxId = expectedItems.stream()
                .map(TestItem::getId)
                .max(Integer::compareTo)
                .orElseThrow(() -> new IllegalStateException("Not items in query response"));
        assertThat(maxId, is(49));
    }

    @Test
    public void testSelectItemWithDescSortOrder() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        List<TestItem> expectedItems = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
            expectedItems.add(testItem);
        }

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .sort("id", true)
                .execute();

        List<TestItem> resultItems = new ArrayList<>();
        while (iterator.hasNext()) {
            resultItems.add(iterator.next());
        }

        for (int i = 0; i < expectedItems.size(); i++) {
            assertThat(expectedItems.get(i).equals(resultItems.get(resultItems.size() - 1 - i)), is(true));
        }
    }

    @Test
    public void testSelectItemWithDescSortOrderWithTopValues() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        List<TestItem> expectedItems = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
            expectedItems.add(testItem);
        }

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .sort("id", false, 99, 98, 97)
                .execute();

        List<TestItem> resultItems = new ArrayList<>();
        while (iterator.hasNext()) {
            resultItems.add(iterator.next());
        }

        for (int i = 0; i < expectedItems.size(); i++) {
            if (i < 3) {
                assertThat(resultItems.get(i), is(expectedItems.get(expectedItems.size() - 1 - i)));
            } else {
                assertThat(expectedItems.get(i - 3).equals(resultItems.get(i)), is(true));
            }
        }
    }

    @Test
    public void testSelectItemListWithFetchCount_1() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        Set<TestItem> expectedItems = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
            expectedItems.add(testItem);
        }

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .fetchCount(1)
                .execute();

        while (iterator.hasNext()) {
            TestItem responseItem = iterator.next();
            assertThat(expectedItems.remove(responseItem), is(true));
        }

        assertThat(expectedItems.size(), is(0));
    }

    @Test
    public void testUpdateOneItem() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        final String updatedName = "updated";
        db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .set("name", updatedName)
                .update();

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("name", EQ, updatedName)
                .execute();

        assertThat(iterator.hasNext(), is(true));
        TestItem updatedItem = iterator.next();
        assertThat(updatedItem.name, is(updatedName));
    }

    @Test
    public void testUpdateItemObjectField() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            db.upsert(namespaceName, testItem);
        }

        NestedTest nested = new NestedTest();
        nested.test = 5;
        nested.value = "updated";
        nested.nonIndex = "updatedNonIndex";
        db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .set("nestedTest", nested)
                .update();

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .execute();

        assertThat(iterator.hasNext(), is(true));
        TestItem updatedItem = iterator.next();

        NestedTest result = updatedItem.nestedTest;
        assertThat(result.test, is(nested.test));
        assertThat(result.value, is(nested.value));
        assertThat(result.nonIndex, is(nested.nonIndex));
    }

    @Test
    public void testUpdateItemObjectFieldToNull() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            db.upsert(namespaceName, testItem);
        }

        NestedTest nested = new NestedTest();
        nested.test = 5;
        nested.value = "updated";
        nested.nonIndex = "updatedNonIndex";
        db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .set("nestedTest", null)
                .update();

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .execute();

        assertThat(iterator.hasNext(), is(true));
        TestItem updatedItem = iterator.next();

        NestedTest result = updatedItem.nestedTest;
        assertThat(result, is(nullValue()));
    }

    @Test
    public void testUpdateItemListObjectToEmptyList() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setListNested(Arrays.asList(new NestedTest(), new NestedTest(), new NestedTest()));
            db.upsert(namespaceName, testItem);
        }

        db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .set("listNested", Collections.emptyList())
                .update();

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .execute();

        assertThat(iterator.hasNext(), is(true));
        TestItem updatedItem = iterator.next();

        assertThat(updatedItem.getListNested().isEmpty(), is(true));
    }

    @Test
    public void testUpdateItemListObjectToListWithSingleElement() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setListNested(Arrays.asList(new NestedTest(), new NestedTest(), new NestedTest()));
            db.upsert(namespaceName, testItem);
        }

        db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .set("listNested", Collections.singletonList(new NestedTest()))
                .update();

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .execute();

        assertThat(iterator.hasNext(), is(true));
        TestItem updatedItem = iterator.next();

        assertThat(updatedItem.getListNested().size(), is(1));
    }

    @Test
    public void testUpdateItemListObjects() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setListNested(Arrays.asList(new NestedTest(), new NestedTest(), new NestedTest()));
            db.upsert(namespaceName, testItem);
        }

        NestedTest nestedTest1 = new NestedTest();
        nestedTest1.test = 1;
        nestedTest1.value = "val1";
        nestedTest1.nonIndex = "non1";
        NestedTest nestedTest2 = new NestedTest();
        nestedTest2.test = 2;
        nestedTest2.value = "val2";
        nestedTest2.nonIndex = "non2";
        db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .set("listNested", Arrays.asList(nestedTest1, nestedTest2))
                .update();

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .execute();

        assertThat(iterator.hasNext(), is(true));
        TestItem updatedItem = iterator.next();

        assertThat(updatedItem.getListNested().size(), is(2));
    }

    @Test
    public void testUpdateItemFieldListPrimitives() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setIntegers(Arrays.asList(5, 4, 3, 2, 1));
            db.upsert(namespaceName, testItem);
        }

        db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .set("integers", Arrays.asList(55, 44, 33, 22, 11))
                .update();

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .execute();

        assertThat(iterator.hasNext(), is(true));
        TestItem updatedItem = iterator.next();

        assertThat(updatedItem.integers, contains(55, 44, 33, 22, 11));
    }

    @Test
    public void testUpdateItemListPrimitivesToEmpty() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setIntegers(Arrays.asList(5, 4, 3, 2, 1));
            db.upsert(namespaceName, testItem);
        }

        db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .set("integers", Collections.emptyList())
                .update();

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .execute();

        assertThat(iterator.hasNext(), is(true));
        TestItem updatedItem = iterator.next();

        assertThat(updatedItem.integers.isEmpty(), is(true));
    }

    @Test
    public void testInsertItemWithDoubleValue() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setDoubleValue((double) i);
            db.upsert(namespaceName, testItem);
        }

        CloseableIterator<TestItem> execute = db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .execute();

        TestItem result = execute.next();
        assertThat(result.doubleValue, is(77.0D));
    }

    @Test
    public void testUpdateItemDoubleValue() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            db.upsert(namespaceName, testItem);
        }

       db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .set("doubleValue", 123.123D)
                .update();

        CloseableIterator<TestItem> items = db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .execute();

        TestItem result = items.next();
        assertThat(result.doubleValue, is(123.123D));
    }

    @Test
    public void testUpdateFieldToNullItem() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            testItem.setNonIndex("nonIndex" + i);
            db.upsert(namespaceName, testItem);
        }

        db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .set("nonIndex", null)
                .update();

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .execute();

        assertThat(iterator.hasNext(), is(true));
        TestItem updatedItem = iterator.next();
        assertThat(updatedItem.nonIndex, is(nullValue()));
    }

    @Test
    public void testDropFieldToNullItem() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            testItem.setNonIndex("nonIndex" + i);
            db.upsert(namespaceName, testItem);
        }

        db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .drop("nonIndex")
                .update();

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .execute();

        assertThat(iterator.hasNext(), is(true));
        TestItem updatedItem = iterator.next();
        assertThat(updatedItem.nonIndex, is(nullValue()));
    }

    @Test
    public void testUpdateItemList() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        final String updatedName = "updated";
        db.query("items", TestItem.class)
                .where("id", EQ, 77, 17, 7)
                .set("name", updatedName)
                .update();

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("name", EQ, updatedName)
                .execute();

        int updateCount = 0;
        while (iterator.hasNext()) {
            TestItem updatedItem = iterator.next();
            assertThat(updatedItem.name, is(updatedName));
            updateCount++;
        }

        assertThat(updateCount, is(3));
    }

    @Test
    public void testUpdateAllItems() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        final String updatedName = "updated";
        db.query("items", TestItem.class)
                .set("name", updatedName)
                .update();

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("name", EQ, updatedName)
                .execute();

        int updateCount = 0;
        while (iterator.hasNext()) {
            TestItem updatedItem = iterator.next();
            assertThat(updatedItem.name, is(updatedName));
            updateCount++;
        }

        assertThat(updateCount, is(100));
    }

    @Test
    public void testUpdateTwoFieldsOnOneItem() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        String updatedName = "updatedName";
        String updatedValue = "updatedValue";

        db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .set("name", updatedName)
                .set("value", updatedValue)
                .update();

        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("name", EQ, updatedName)
                .where("value", EQ, updatedValue)
                .execute();

        int updateCount = 0;
        while (iterator.hasNext()) {
            TestItem updatedItem = iterator.next();
            assertThat(updatedItem.name, is(updatedName));
            assertThat(updatedItem.value, is(updatedValue));
            updateCount++;
        }

        assertThat(updateCount, is(1));
    }

    @Test
    public void testUpsertItemWithNestedObject() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        NestedTest nestedTest = new NestedTest();
        nestedTest.value = "nestedValue";
        nestedTest.test = 123;

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        testItem.setNestedTest(nestedTest);

        db.upsert(namespaceName, testItem);

        ItemsResponse itemsResponse = get("/db/test_items/namespaces/items/items", ItemsResponse.class);
        assertThat(itemsResponse.totalItems, is(1));
        TestItem responseItem = itemsResponse.items.get(0);
        assertThat(responseItem.name, is(testItem.name));
        assertThat(responseItem.id, is(testItem.id));
        NestedTest responseNestedTest = responseItem.getNestedTest();
        assertThat(responseNestedTest.value, is(nestedTest.value));
        assertThat(responseNestedTest.test, is(nestedTest.test));

    }

    @Test
    public void testTransactionInsertWithCommit() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        tx.insert(testItem);

        tx.commit();

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .execute();
        assertThat(iterator.hasNext(), is(true));

        TestItem item = iterator.next();
        assertThat(item.id, is(testItem.id));
        assertThat(item.name, is(testItem.name));
        assertThat(item.nonIndex, is(testItem.nonIndex));
    }

    @Test
    public void testTransactionInsertMultithreadingWithCommit() throws InterruptedException {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), SerialIdTestItem.class);

        int threadPoolSize = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
        CountDownLatch latch = new CountDownLatch(threadPoolSize);
        for (int i = 0; i < threadPoolSize; i++) {
            executor.execute(() -> {
                Transaction<SerialIdTestItem> tx = db.beginTransaction(namespaceName, SerialIdTestItem.class);
                try {
                    for (int j = 0; j < 1000; j++) {
                        tx.insert(new SerialIdTestItem());
                    }
                    tx.commit();
                } catch (Exception e) {
                    tx.rollback();
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        executor.shutdown();

        try (Stream<SerialIdTestItem> items = db.query(namespaceName, SerialIdTestItem.class).stream()) {
            List<Integer> ids = items.map(SerialIdTestItem::getId).collect(Collectors.toList());
            assertThat(ids, hasSize(10000));
            assertThat(ids, contains(IntStream.rangeClosed(1, 10000).boxed().toArray()));
        }
    }

    @Test
    public void testTransactionInsertWithRollback() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        tx.insert(testItem);

        tx.rollback();

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .execute();
        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testTransactionUpsertWithCommit() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        tx.upsert(testItem);

        tx.commit();

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .execute();
        assertThat(iterator.hasNext(), is(true));

        TestItem item = iterator.next();
        assertThat(item.id, is(testItem.id));
        assertThat(item.name, is(testItem.name));
        assertThat(item.nonIndex, is(testItem.nonIndex));
    }

    @Test
    public void testTransactionUpsertWithRollback() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        tx.upsert(testItem);

        tx.rollback();

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .execute();
        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testTransactionUpdateWithCommit() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        db.upsert(namespaceName, testItem);

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        testItem.setName("TestNameUpdated");
        tx.update(testItem);

        tx.commit();

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .execute();
        assertThat(iterator.hasNext(), is(true));

        TestItem item = iterator.next();
        assertThat(item.id, is(testItem.id));
        assertThat(item.name, is(testItem.name));
        assertThat(item.nonIndex, is(testItem.nonIndex));
    }

    @Test
    public void testTransactionUpdateWithRollback() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        db.upsert(namespaceName, testItem);

        String originalName = testItem.getName();

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        testItem.setName("TestNameUpdated");
        tx.update(testItem);

        tx.rollback();

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .execute();
        assertThat(iterator.hasNext(), is(true));

        TestItem item = iterator.next();
        assertThat(item.id, is(testItem.id));
        assertThat(item.name, is(originalName));
        assertThat(item.nonIndex, is(testItem.nonIndex));
    }

    @Test
    public void testTransactionUpdateOnUncommittedItem() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        tx.insert(testItem);

        testItem.setName("TestNameUpdated");
        tx.update(testItem);

        tx.commit();

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .execute();
        assertThat(iterator.hasNext(), is(true));

        TestItem item = iterator.next();
        assertThat(item.id, is(testItem.id));
        assertThat(item.name, is(testItem.name));
        assertThat(item.nonIndex, is(testItem.nonIndex));
    }

    @Test
    public void testTransactionDeleteWithCommit() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        db.upsert(namespaceName, testItem);

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        tx.delete(testItem);

        tx.commit();

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .execute();
        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testTransactionDeleteWithRollback() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        db.upsert(namespaceName, testItem);

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        tx.delete(testItem);

        tx.rollback();

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .execute();
        assertThat(iterator.hasNext(), is(true));

        TestItem item = iterator.next();
        assertThat(item.id, is(testItem.id));
        assertThat(item.name, is(testItem.name));
        assertThat(item.nonIndex, is(testItem.nonIndex));
    }

    @Test
    public void testTransactionDeleteOnUncommittedItem() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        tx.insert(testItem);

        tx.delete(testItem);

        tx.commit();

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .execute();
        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testTransactionUpdateQueryWithCommit() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        db.upsert(namespaceName, testItem);

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        tx.query()
                .where("id", EQ, 123)
                .set("name", "TestNameUpdated")
                .update();
        tx.commit();

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .execute();
        assertThat(iterator.hasNext(), is(true));

        TestItem item = iterator.next();
        assertThat(item.id, is(testItem.id));
        assertThat(item.name, is("TestNameUpdated"));
        assertThat(item.nonIndex, is(testItem.nonIndex));
    }

    @Test
    public void testTransactionUpdateQueryWithRollback() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        db.upsert(namespaceName, testItem);

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        tx.query()
                .where("id", EQ, 123)
                .set("name", "TestNameUpdated")
                .update();
        tx.rollback();

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .execute();
        assertThat(iterator.hasNext(), is(true));

        TestItem item = iterator.next();
        assertThat(item.id, is(testItem.id));
        assertThat(item.name, is(testItem.name));
        assertThat(item.nonIndex, is(testItem.nonIndex));
    }

    @Test
    public void testTransactionUpdateQueryOnUncommittedItem() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        tx.upsert(testItem);

        tx.query()
                .where("id", EQ, 123)
                .set("name", "TestNameUpdated")
                .update();

        tx.commit();

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .execute();
        assertThat(iterator.hasNext(), is(true));

        TestItem item = iterator.next();
        assertThat(item.id, is(testItem.id));
        assertThat(item.name, is("TestNameUpdated"));
        assertThat(item.nonIndex, is(testItem.nonIndex));
    }

    @Test
    public void testTransactionDeleteQueryWithCommit() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        db.upsert(namespaceName, testItem);

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        tx.query()
                .where("id", EQ, 123)
                .delete();
        tx.commit();

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .execute();
        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testTransactionDeleteQueryWithRollback() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        db.upsert(namespaceName, testItem);

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        tx.query()
                .where("id", EQ, 123)
                .delete();
        tx.rollback();

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .execute();
        assertThat(iterator.hasNext(), is(true));

        TestItem item = iterator.next();
        assertThat(item.id, is(testItem.id));
        assertThat(item.name, is(testItem.name));
        assertThat(item.nonIndex, is(testItem.nonIndex));
    }

    @Test
    public void testTransactionDeleteQueryOnUncommittedItem() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        tx.upsert(testItem);

        tx.query()
                .where("id", EQ, 123)
                .delete();

        tx.commit();

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .execute();
        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testUpsertWithSerialId() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), SerialIdTestItem.class);

        db.upsert(namespaceName, new SerialIdTestItem());
        db.upsert(namespaceName, new SerialIdTestItem());
        db.upsert(namespaceName, new SerialIdTestItem());

        Iterator<SerialIdTestItem> iterator = db.query(namespaceName, SerialIdTestItem.class)
                .where("id", EQ, 1, 2, 3)
                .execute();
        List<Integer> ids = new ArrayList<>();
        while (iterator.hasNext()) {
            SerialIdTestItem item = iterator.next();
            ids.add(item.id);
        }
        assertThat(ids, contains(1, 2, 3));
    }

    @Test
    public void testTransactionUpsertWithSerialId() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), SerialIdTestItem.class);

        Transaction<SerialIdTestItem> tx = db.beginTransaction(namespaceName, SerialIdTestItem.class);
        tx.upsert(new SerialIdTestItem());
        tx.upsert(new SerialIdTestItem());
        tx.upsert(new SerialIdTestItem());
        tx.commit();

        Iterator<SerialIdTestItem> iterator = db.query(namespaceName, SerialIdTestItem.class)
                .where("id", EQ, 1, 2, 3)
                .execute();
        List<Integer> ids = new ArrayList<>();
        while (iterator.hasNext()) {
            SerialIdTestItem item = iterator.next();
            ids.add(item.id);
        }
        assertThat(ids, contains(1, 2, 3));
    }

    @Test
    public void testTransactionInsertAsyncWithCommit() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        List<TestItem> results = new CopyOnWriteArrayList<>();

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setNonIndex("testNonIndex" + i);
            tx.insertAsync(testItem).thenAccept(results::add);
        }

        tx.commit();

        List<TestItem> items = db.query(namespaceName, TestItem.class).toList();
        assertThat(items, hasSize(100));
        assertThat(results, hasSize(100));

        assertThat(items, containsInAnyOrder(results.toArray()));
    }

    @Test
    public void testTransactionInsertAsyncMultithreadingWithCommit() throws InterruptedException {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), SerialIdTestItem.class);

        int threadPoolSize = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
        CountDownLatch latch = new CountDownLatch(threadPoolSize);
        for (int i = 0; i < threadPoolSize; i++) {
            executor.execute(() -> {
                Transaction<SerialIdTestItem> tx = db.beginTransaction(namespaceName, SerialIdTestItem.class);
                try {
                    for (int j = 0; j < 1000; j++) {
                        tx.insertAsync(new SerialIdTestItem());
                    }
                    tx.commit();
                } catch (Exception e) {
                    tx.rollback();
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        executor.shutdown();

        try (Stream<SerialIdTestItem> items = db.query(namespaceName, SerialIdTestItem.class).stream()) {
            List<Integer> ids = items.map(SerialIdTestItem::getId).collect(Collectors.toList());
            assertThat(ids, hasSize(10000));
            assertThat(ids, contains(IntStream.rangeClosed(1, 10000).boxed().toArray()));
        }
    }

    @Test
    public void testTransactionInsertAsyncWithRollback() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        List<TestItem> results = new CopyOnWriteArrayList<>();

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setNonIndex("testNonIndex" + i);
            tx.insertAsync(testItem).thenAccept(results::add);
        }

        tx.rollback();

        assertThat(results, hasSize(100));

        List<TestItem> items = db.query(namespaceName, TestItem.class).toList();
        assertThat(items, empty());
    }

    @Test
    public void testTransactionUpdateAsyncWithCommit() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        db.insert(namespaceName, testItem);

        List<TestItem> results = new CopyOnWriteArrayList<>();

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        testItem.setName("TestNameUpdated");
        tx.updateAsync(testItem).thenAccept(results::add);

        tx.commit();

        assertThat(results, hasSize(1));
        assertThat(results, contains(testItem));

        TestItem item = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .getOne();
        assertThat(item.id, is(testItem.id));
        assertThat(item.name, is(testItem.name));
        assertThat(item.nonIndex, is(testItem.nonIndex));
    }

    @Test
    public void testTransactionUpdateAsyncWithRollback() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        db.insert(namespaceName, testItem);

        String originalName = testItem.getName();

        List<TestItem> results = new CopyOnWriteArrayList<>();

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        testItem.setName("TestNameUpdated");
        tx.updateAsync(testItem).thenAccept(results::add);

        tx.rollback();

        assertThat(results, hasSize(1));
        assertThat(results, contains(testItem));

        TestItem item = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .getOne();
        assertThat(item.id, is(testItem.id));
        assertThat(item.name, is(originalName));
        assertThat(item.nonIndex, is(testItem.nonIndex));
    }

    @Test
    public void testTransactionUpsertAsyncWithCommit() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        List<TestItem> results = new CopyOnWriteArrayList<>();

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        tx.upsertAsync(testItem).thenAccept(results::add);

        tx.commit();

        assertThat(results, hasSize(1));
        assertThat(results, contains(testItem));

        TestItem item = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .getOne();
        assertThat(item.id, is(testItem.id));
        assertThat(item.name, is(testItem.name));
        assertThat(item.nonIndex, is(testItem.nonIndex));
    }

    @Test
    public void testTransactionUpsertAsyncWithRollback() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        List<TestItem> results = new CopyOnWriteArrayList<>();

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        tx.upsertAsync(testItem).thenAccept(results::add);

        tx.rollback();

        assertThat(results, hasSize(1));
        assertThat(results, contains(testItem));

        Optional<TestItem> item = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .findOne();
        assertThat(item.isPresent(), is(false));
    }

    @Test
    public void testTransactionDeleteAsyncWithCommit() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        db.insert(namespaceName, testItem);

        List<TestItem> results = new CopyOnWriteArrayList<>();

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        tx.deleteAsync(testItem).thenAccept(results::add);

        tx.commit();

        assertThat(results, hasSize(1));
        assertThat(results, contains(testItem));

        Optional<TestItem> item = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .findOne();
        assertThat(item.isPresent(), is(false));
    }

    @Test
    public void testTransactionDeleteAsyncWithRollback() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        db.insert(namespaceName, testItem);

        List<TestItem> results = new CopyOnWriteArrayList<>();

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        tx.deleteAsync(testItem).thenAccept(results::add);

        tx.rollback();

        assertThat(results, hasSize(1));
        assertThat(results, contains(testItem));

        TestItem item = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .getOne();
        assertThat(item.id, is(testItem.id));
        assertThat(item.name, is(testItem.name));
        assertThat(item.nonIndex, is(testItem.nonIndex));
    }

    @Test
    public void testQueryToList() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        List<TestItem> testItems = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setNonIndex("testNonIndex" + i);
            db.insert(namespaceName, testItem);
            testItems.add(testItem);
        }
        assertThat(testItems, hasSize(5));

        List<TestItem> items = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 0, 1, 2, 3, 4)
                .toList();
        assertThat(items, hasSize(5));

        for (int i = 0; i < 5; i++) {
            TestItem item = items.get(i);
            TestItem testItem = testItems.get(i);
            assertThat(item.id, is(testItem.id));
            assertThat(item.name, is(testItem.name));
            assertThat(item.nonIndex, is(testItem.nonIndex));
        }
    }

    @Test
    public void testQueryGetOne() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        db.insert(namespaceName, testItem);

        TestItem item = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .getOne();
        assertThat(item.id, is(testItem.id));
        assertThat(item.name, is(testItem.name));
        assertThat(item.nonIndex, is(testItem.nonIndex));
    }

    @Test
    public void testQueryGetOneWhenNoItemThenException() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        assertThrows(RuntimeException.class,
                () -> db.query(namespaceName, TestItem.class)
                        .where("id", EQ, 123)
                        .getOne(),
                "Exactly one item expected, but there is zero");
    }

    @Test
    public void testQueryGetOneWhenMoreThanOneItemThenException() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        for (int i = 0; i < 2; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setNonIndex("testNonIndex" + i);
            db.insert(namespaceName, testItem);
        }

        assertThrows(RuntimeException.class, () -> db.query(namespaceName, TestItem.class).getOne(),
                "Exactly one item expected, but there are more");
    }

    @Test
    public void testQueryFindOne() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        db.insert(namespaceName, testItem);

        Optional<TestItem> itemOptional = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .findOne();
        assertThat(itemOptional.isPresent(), is(true));

        TestItem item = itemOptional.get();
        assertThat(item.id, is(testItem.id));
        assertThat(item.name, is(testItem.name));
        assertThat(item.nonIndex, is(testItem.nonIndex));
    }

    @Test
    public void testQueryFindOneWhenNoItemThenEmpty() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        Optional<TestItem> itemOptional = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .findOne();
        assertThat(itemOptional.isPresent(), is(false));
    }

    @Test
    public void testQueryFindOneWhenMoreThanOneItemThenException() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        for (int i = 0; i < 2; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setNonIndex("testNonIndex" + i);
            db.insert(namespaceName, testItem);
        }

        assertThrows(RuntimeException.class, () -> db.query(namespaceName, TestItem.class).findOne(),
                "Exactly one item expected, but there are more");
    }

    @Test
    public void testQueryStream() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        for (int i = 0; i < 5; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setNonIndex("testNonIndex" + i);
            db.insert(namespaceName, testItem);
        }

        try (Stream<TestItem> items = db.query(namespaceName, TestItem.class).stream()) {
            List<Integer> ids = items.map(TestItem::getId).collect(Collectors.toList());
            assertThat(ids, contains(0, 1, 2, 3, 4));
        }
    }

    @Test
    public void testQueryCount() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        for (int i = 0; i < 10; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setNonIndex("testNonIndex" + i);
            db.insert(namespaceName, testItem);
        }

        long count = db.query(namespaceName, TestItem.class).count();
        assertThat(count, is(10L));
    }

    @Test
    public void testQueryNotExists() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        boolean notExists = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .notExists();
        assertThat(notExists, is(true));
    }

    @Test
    public void testQueryExists() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        db.insert(namespaceName, testItem);

        boolean exists = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .exists();
        assertThat(exists, is(true));
    }

    private void post(String path, Object body) {
        HttpPost httpPost = new HttpPost("http://localhost:" + restApiPort + "/api/v1" + path);


        try (CloseableHttpClient client = HttpClients.createDefault()) {
            Gson gson = new GsonBuilder()
                    .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                    .create();
            String json = gson.toJson(body);
            httpPost.setEntity(new StringEntity(json));
            client.execute(httpPost);
        } catch (IOException e) {
            throw new RuntimeException(e.getLocalizedMessage(), e);
        }
    }

    private <T> T get(String path, Class<T> clazz) {
        HttpGet httpGet = new HttpGet("http://localhost:" + restApiPort + "/api/v1" + path);

        try (CloseableHttpClient client = HttpClients.createDefault();
             CloseableHttpResponse response = client.execute(httpGet)) {
            InputStream content = response.getEntity().getContent();
            Gson gson = new GsonBuilder()
                    .create();
            return gson.fromJson(new InputStreamReader(content), clazz);
        } catch (IOException e) {
            throw new RuntimeException(e.getLocalizedMessage(), e);
        }
    }

    public static class CreateDatabase {

        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class SerialIdTestItem {

        @Serial
        @Reindex(name = "id", isPrimaryKey = true)
        private int id;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

    }

    @Reindex(name = "composite", subIndexes = {"id", "name"})
    public static class TestItem {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;
        @Reindex(name = "name")
        private String name;
        @Reindex(name = "value")
        private String value;
        private String nonIndex;
        @Reindex(name = "nestedTest")
        private NestedTest nestedTest;
        @Reindex(name = "listNested")
        private List<NestedTest> listNested;
        @Reindex(name = "integers")
        private List<Integer> integers;
        private Double doubleValue;

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

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public String getNonIndex() {
            return nonIndex;
        }

        public void setNonIndex(String nonIndex) {
            this.nonIndex = nonIndex;
        }

        public NestedTest getNestedTest() {
            return nestedTest;
        }

        public void setNestedTest(NestedTest nestedTest) {
            this.nestedTest = nestedTest;
        }

        public List<NestedTest> getListNested() {
            return listNested;
        }

        public void setListNested(List<NestedTest> listNested) {
            this.listNested = listNested;
        }

        public List<Integer> getIntegers() {
            return integers;
        }

        public void setIntegers(List<Integer> integers) {
            this.integers = integers;
        }

        public Double getDoubleValue() {
            return doubleValue;
        }

        public void setDoubleValue(Double doubleValue) {
            this.doubleValue = doubleValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TestItem)) return false;
            TestItem testItem = (TestItem) o;
            return Objects.equals(id, testItem.id) &&
                   Objects.equals(name, testItem.name) &&
                   Objects.equals(value, testItem.value) &&
                   Objects.equals(nonIndex, testItem.nonIndex) &&
                   Objects.equals(nestedTest, testItem.nestedTest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, value, nonIndex, nestedTest);
        }

        @Override
        public String toString() {
            return "TestItem{" +
                   "id=" + id +
                   ", name='" + name + '\'' +
                   ", value='" + value + '\'' +
                   ", nonIndex='" + nonIndex + '\'' +
                   ", nestedTest=" + nestedTest +
                   ", listNested=" + listNested +
                   ", integers=" + integers +
                   '}';
        }

    }

    public static class NestedTest {

        @Reindex(name = "value")
        private String value;
        @Reindex(name = "test")
        private Integer test;
        private String nonIndex;

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public Integer getTest() {
            return test;
        }

        public void setTest(Integer test) {
            this.test = test;
        }

        public String getNonIndex() {
            return nonIndex;
        }

        public void setNonIndex(String nonIndex) {
            this.nonIndex = nonIndex;
        }
    }

    public static class ItemsResponse {
        @SerializedName("total_items")
        private int totalItems;
        private List<TestItem> items;

        public int getTotalItems() {
            return totalItems;
        }

        public void setTotalItems(int totalItems) {
            this.totalItems = totalItems;
        }

        public List<TestItem> getItems() {
            return items;
        }

        public void setItems(List<TestItem> items) {
            this.items = items;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class NamespaceResponse {
        private String name;
        private StorageResponse storage;
        private List<IndexResponse> indexes;

        @JsonIgnoreProperties(ignoreUnknown = true)
        private static class StorageResponse {
            private boolean enabled;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public StorageResponse getStorage() {
            return storage;
        }

        public void setStorage(StorageResponse storage) {
            this.storage = storage;
        }

        public List<IndexResponse> getIndexes() {
            return indexes;
        }

        public void setIndexes(List<IndexResponse> indexes) {
            this.indexes = indexes;
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        private static class IndexResponse {
            private String name;
            @SerializedName("json_paths")
            private List<String> jsonPaths;
            @SerializedName("field_type")
            private String fieldType;
            @SerializedName("index_type")
            private String indexType;
            @SerializedName("is_pk")
            private boolean isPk;
            @SerializedName("is_array")
            private boolean isArray;
            @SerializedName("is_dense")
            private boolean isDense;
            @SerializedName("is_sparse")
            private boolean isSparse;
            @SerializedName("is_linear")
            private boolean isLinear;
            @SerializedName("is_simple_tag")
            private boolean isSimpleTag;
            @SerializedName("collate_mode")
            private String collateMode;
            @SerializedName("sort_order_letters")
            private String sortOrderLetters;

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }

            public List<String> getJsonPaths() {
                return jsonPaths;
            }

            public void setJsonPaths(List<String> jsonPaths) {
                this.jsonPaths = jsonPaths;
            }

            public String getFieldType() {
                return fieldType;
            }

            public void setFieldType(String fieldType) {
                this.fieldType = fieldType;
            }

            public String getIndexType() {
                return indexType;
            }

            public void setIndexType(String indexType) {
                this.indexType = indexType;
            }

            public boolean isPk() {
                return isPk;
            }

            public void setPk(boolean pk) {
                isPk = pk;
            }

            public boolean isArray() {
                return isArray;
            }

            public void setArray(boolean array) {
                isArray = array;
            }

            public boolean isDense() {
                return isDense;
            }

            public void setDense(boolean dense) {
                isDense = dense;
            }

            public boolean isSparse() {
                return isSparse;
            }

            public void setSparse(boolean sparse) {
                isSparse = sparse;
            }

            public boolean isLinear() {
                return isLinear;
            }

            public void setLinear(boolean linear) {
                isLinear = linear;
            }

            public boolean isSimpleTag() {
                return isSimpleTag;
            }

            public void setSimpleTag(boolean simpleTag) {
                isSimpleTag = simpleTag;
            }

            public String getCollateMode() {
                return collateMode;
            }

            public void setCollateMode(String collateMode) {
                this.collateMode = collateMode;
            }

            public String getSortOrderLetters() {
                return sortOrderLetters;
            }

            public void setSortOrderLetters(String sortOrderLetters) {
                this.sortOrderLetters = sortOrderLetters;
            }
        }
    }

}
