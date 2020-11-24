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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import ru.rt.restream.reindexer.Configuration;
import ru.rt.restream.reindexer.Reindexer;
import ru.rt.restream.reindexer.annotations.Namespace;
import ru.rt.restream.reindexer.annotations.Reindex;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.is;
import static ru.rt.restream.reindexer.Index.Option.PK;
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
                .getReindexer();
    }

    @Test
    public void testOpenNamespace() {
        String namespaceName = "items";

        db.openNamespace(namespaceName, TestItem.class);

        NamespaceResponse namespaceResponse = get("/db/test_items/namespaces/items", NamespaceResponse.class);
        assertThat(namespaceResponse.name, is(namespaceName));
        assertThat(namespaceResponse.indexes.size(), is(3));
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
    public void testUpsertItem() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");

        db.upsert(namespaceName, testItem);

        ItemsResponse itemsResponse = get("/db/test_items/namespaces/items/items", ItemsResponse.class);
        assertThat(itemsResponse.totalItems, is(1));
        TestItem responseItem = itemsResponse.items.get(0);
        assertThat(responseItem.name, is(testItem.name));
        assertThat(responseItem.id, is(testItem.id));
    }

    @Test
    public void testSelectOneItem() {
        //Вставить 100 элементов
        String namespaceName = "items";
        db.openNamespace(namespaceName, TestItem.class);
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
    public void testDeleteOneItem() {
        //Вставить 100 элементов
        String namespaceName = "items";
        db.openNamespace(namespaceName, TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        //Удалить из БД элемент с id 77
        long delete = db.query("items", TestItem.class)
                .where("id", EQ, 77)
                .delete();

        assertThat(delete, is(1L));

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
        db.openNamespace(namespaceName, TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        //Удалить из БД элементы с id 77, 17, 7
        long delete = db.query("items", TestItem.class)
                .where("id", EQ, 77, 17, 7)
                .delete();

        assertThat(delete, is(3L));

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
        db.openNamespace(namespaceName, TestItem.class);
        for (int i = 0; i < 100; i++) {
            TestItem testItem = new TestItem();
            testItem.setId(i);
            testItem.setName("TestName" + i);
            testItem.setValue(i + "Value");
            db.upsert(namespaceName, testItem);
        }

        //Удалить из БД элементы с id 77, 17, 7
        long delete = db.query("items", TestItem.class)
                .delete();

        assertThat(delete, is(100L));

        //Выбрать из БД элементы с id 77, 17, 7
        Iterator<TestItem> iterator = db.query("items", TestItem.class)
                .execute();

        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testSelectItemList() {
        //Вставить 100 элементов
        String namespaceName = "items";
        db.openNamespace(namespaceName, TestItem.class);

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
        db.openNamespace(namespaceName, TestItem.class);

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
        db.openNamespace(namespaceName, TestItem.class);

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
        db.openNamespace(namespaceName, TestItem.class);

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
        db.openNamespace(namespaceName, TestItem.class);

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
        db.openNamespace(namespaceName, TestItem.class);

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
                    .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                    .create();
            return gson.fromJson(new InputStreamReader(content), clazz);
        } catch (IOException e) {
            throw new RuntimeException(e.getLocalizedMessage(), e);
        }
    }

    @Getter
    @Setter
    public static class CreateDatabase {

        private String name;

    }

    @Getter
    @Setter
    @ToString
    @EqualsAndHashCode
    @Namespace
    public static class TestItem {
        @Reindex(options = PK)
        private Integer id;
        @Reindex(name = "name")
        private String name;
        @Reindex(name = "value")
        private String value;
    }

    @Getter
    @Setter
    public static class ItemsResponse {
        private int totalItems;
        private List<TestItem> items;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @Getter
    @Setter
    private static class NamespaceResponse {
        private String name;
        private StorageResponse storage;
        private List<IndexResponse> indexes;

        @JsonIgnoreProperties(ignoreUnknown = true)
        @Getter
        @Setter
        private static class StorageResponse {
            private boolean enabled;
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        @Getter
        @Setter
        private static class IndexResponse {
            private String name;
            private List<String> jsonPaths;
            private String fieldType;
            private String indexType;
            private boolean isPk;
            private boolean isArray;
            private boolean isDense;
            private boolean isSparse;
            private boolean isLinear;
            private boolean isSimpleTag;
            private String collateMode;
            private String sortOrderLetters;
        }
    }

}