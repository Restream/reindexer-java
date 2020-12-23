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
import ru.rt.restream.reindexer.Configuration;
import ru.rt.restream.reindexer.DeferredResult;
import ru.rt.restream.reindexer.Reindexer;
import ru.rt.restream.reindexer.Transaction;
import ru.rt.restream.reindexer.annotations.Reindex;
import ru.rt.restream.reindexer.annotations.Serial;
import ru.rt.restream.reindexer.binding.option.NamespaceOptions;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static ru.rt.restream.reindexer.Query.Condition.EQ;

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
                .threadPoolSize(1)
                .connectionPoolSize(4)
                .connectionTimeout(30L)
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
        //Вставить 100 элементов
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        //Выбрать из БД элемент с id 77
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
    public void testSelectOneItemByCompositeIndex() {
        //Вставить 100 элементов
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        //Выбрать из БД элемент с id 77
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
        //Вставить 100 элементов
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

        //Выбрать из БД элемент с id 77
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
        //Вставить 100 элементов
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

        //Выбрать из БД элемент с id 77
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
        //Вставить 100 элементов
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

        //Выбрать из БД элемент с id 77
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
        //Вставить 100 элементов
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        //Выбрать из БД элемент с id 77
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
        //Вставить 100 элементов
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        //Выбрать из БД элемент с id 77
        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .where("name", EQ, "TestName77")
                .where("value", EQ, "notEquals")
                .execute();

        assertThat(iterator.hasNext(), is(false));

    }

    @Test
    public void testDeleteOneItem() {
        //Вставить 100 элементов
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        //Удалить из БД элемент с id 77
        db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .delete();

        //Выбрать из БД элемент с id 77
        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .execute();

        assertThat(iterator.hasNext(), is(false));

    }

    @Test
    public void testDeleteListItem() {
        //Вставить 100 элементов
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        //Удалить из БД элементы с id 77, 17, 7
        db.query("items", TestItem.class)
                .where("id", EQ, 77, 17, 7)
                .delete();

        //Выбрать из БД элементы с id 77, 17, 7
        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .where("id", EQ, 77, 17, 7)
                .execute();

        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testDeleteAllItems() {
        //Вставить 100 элементов
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        //Удалить из БД элементы с id 77, 17, 7
        db.query("items", TestItem.class)
                .delete();

        //Выбрать из БД элементы с id 77, 17, 7
        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .execute();

        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testSelectItemList() {
        //Вставить 100 элементов
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
        //Вставить 100 элементов
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
        //Вставить 100 элементов
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
        //Вставить 100 элементов
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
        //Вставить 100 элементов
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
        //Вставить 100 элементов
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
        //Вставить 100 элементов
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        //Обновить поле объекта с id 77
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
    public void testUpdateFieldToNullItem() {
        //Вставить 100 элементов
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

        //Обновить поле объекта с id 77
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
        //Вставить 100 элементов
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

        //Удалить поле объекта с id 77
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
        //Вставить 100 элементов
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        //Обновить поля объектов с id 77, 17, 7
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
        //Вставить 100 элементов
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
        //Вставить 100 элементов
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

        List<DeferredResult<TestItem>> results = new ArrayList<>();

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setNonIndex("testNonIndex" + i);
            tx.insertAsync(testItem, results::add);
        }

        tx.commit();

        int itemCount = 0;
        Iterator<DeferredResult<TestItem>> resultIterator = results.iterator();
        Iterator<TestItem> itemIterator = db.query(namespaceName, TestItem.class).execute();
        while (resultIterator.hasNext() && itemIterator.hasNext()) {
            DeferredResult<TestItem> result = resultIterator.next();
            assertThat(result.hasError(), is(false));

            TestItem testItem = result.getItem();
            assertThat(testItem, notNullValue());

            TestItem item = itemIterator.next();
            assertThat(item.id, is(testItem.id));
            assertThat(item.name, is(testItem.name));
            assertThat(item.nonIndex, is(testItem.nonIndex));

            itemCount++;
        }

        assertThat(itemCount, is(100));
    }

    @Test
    public void testTransactionInsertAsyncWithRollback() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        List<DeferredResult<TestItem>> results = new ArrayList<>();

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setNonIndex("testNonIndex" + i);
            tx.insertAsync(testItem, results::add);
        }

        tx.rollback();

        assertThat(results, hasSize(100));
        assertThat(results.stream().noneMatch(DeferredResult::hasError), is(true));

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class).execute();
        assertThat(iterator.hasNext(), is(false));
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

        List<DeferredResult<TestItem>> results = new ArrayList<>();

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        testItem.setName("TestNameUpdated");
        tx.updateAsync(testItem, results::add);

        tx.commit();

        assertThat(results, hasSize(1));
        assertThat(results.stream().noneMatch(DeferredResult::hasError), is(true));

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
    public void testTransactionUpdateAsyncWithRollback() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        db.insert(namespaceName, testItem);

        String originalName = testItem.getName();

        List<DeferredResult<TestItem>> results = new ArrayList<>();

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        testItem.setName("TestNameUpdated");
        tx.updateAsync(testItem, results::add);

        tx.rollback();

        assertThat(results, hasSize(1));
        assertThat(results.stream().noneMatch(DeferredResult::hasError), is(true));

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
    public void testTransactionUpsertAsyncWithCommit() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        List<DeferredResult<TestItem>> results = new ArrayList<>();

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        tx.upsertAsync(testItem, results::add);

        tx.commit();

        assertThat(results, hasSize(1));
        assertThat(results.stream().noneMatch(DeferredResult::hasError), is(true));

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
    public void testTransactionUpsertAsyncWithRollback() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        List<DeferredResult<TestItem>> results = new ArrayList<>();

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        tx.upsertAsync(testItem, results::add);

        tx.rollback();

        assertThat(results, hasSize(1));
        assertThat(results.stream().noneMatch(DeferredResult::hasError), is(true));

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .execute();
        assertThat(iterator.hasNext(), is(false));
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

        List<DeferredResult<TestItem>> results = new ArrayList<>();

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        tx.deleteAsync(testItem, results::add);

        tx.commit();

        assertThat(results, hasSize(1));
        assertThat(results.stream().noneMatch(DeferredResult::hasError), is(true));

        Iterator<TestItem> iterator = db.query(namespaceName, TestItem.class)
                .where("id", EQ, 123)
                .execute();
        assertThat(iterator.hasNext(), is(false));
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

        List<DeferredResult<TestItem>> results = new ArrayList<>();

        Transaction<TestItem> tx = db.beginTransaction(namespaceName, TestItem.class);
        tx.deleteAsync(testItem, results::add);

        tx.rollback();

        assertThat(results, hasSize(1));
        assertThat(results.stream().noneMatch(DeferredResult::hasError), is(true));

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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
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
    }

    private static class NestedTest {
        @Reindex(name = "value")
        private String value;
        @Reindex(name = "test")
        private Integer test;
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
