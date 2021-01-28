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

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.commons.io.FileUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.rt.restream.reindexer.Configuration;
import ru.rt.restream.reindexer.Query;
import ru.rt.restream.reindexer.Reindexer;
import ru.rt.restream.reindexer.binding.option.NamespaceOptions;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class JsonTest {

    private Reindexer server;

    private Reindexer db;

    @BeforeEach
    public void setUp() {
        server = Configuration.builder()
                .url("builtinserver://items")
                .getReindexer();
        this.db = Configuration.builder()
                .url("cproto://localhost:6534/items")
                .connectionPoolSize(4)
                .requestTimeout(Duration.ofSeconds(30L))
                .getReindexer();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (server != null) {
            server.close();
            FileUtils.deleteDirectory(new File("/tmp/reindex/items"));
        }
        if (db != null) {
            db.close();
        }
    }

    @Test
    public void testInsert() {
        TestItem testItem = new TestItem();
        testItem.id = 1;
        testItem.testValue = "test_value";
        testItem.listIntegers = Arrays.asList(1, 2, 3);
        Nested nested = new Nested("a");
        nested.nested = new Nested("b");
        nested.nestedList = Arrays.asList(new Nested("a"), new Nested("b"), new Nested("c"));
        testItem.listNested = Collections.singletonList(nested);

        db.openNamespace("items", NamespaceOptions.defaultOptions(), TestItem.class);

        db.insert("items", testItem);

        JsonObject items = getItems().getAsJsonObject();
        JsonArray jsonArray = items.get("items").getAsJsonArray();
        JsonObject testItemJson = jsonArray.get(0).getAsJsonObject();

        assertThat(testItemJson.get("i").getAsInt(), is(testItem.id));
        assertThat(testItemJson.get("t_v").getAsString(), is(testItem.testValue));

        JsonArray integersArray = testItemJson.get("l_i").getAsJsonArray();
        assertThat(integersArray.size(), is(testItem.listIntegers.size()));
        for (JsonElement integersValue : integersArray) {
            assertThat(testItem.listIntegers.contains(integersValue.getAsInt()), is(true));
        }

        JsonArray listNestedJson = testItemJson.get("l_n").getAsJsonArray();
        assertThat(listNestedJson.size(), is(testItem.listNested.size()));
        JsonObject nestedListItem = listNestedJson.get(0).getAsJsonObject();
        assertThat(nestedListItem.get("v").getAsString(), is(testItem.listNested.get(0).value));
        assertThat(nestedListItem.get("n_l").getAsJsonArray().size(), is(testItem.listNested.get(0).nestedList.size()));
        JsonObject nestedListItemNestedItem = nestedListItem.get("n").getAsJsonObject();
        assertThat(nestedListItemNestedItem.get("v").getAsString(), is(testItem.listNested.get(0).nested.value));
    }

    @Test
    public void testSelect() {
        TestItem testItem = new TestItem();
        testItem.id = 1;
        testItem.testValue = "test_value";
        testItem.listIntegers = Arrays.asList(1, 2, 3);
        Nested nested = new Nested("a");
        nested.nested = new Nested("b");
        nested.nestedList = Arrays.asList(new Nested("a"), new Nested("b"), new Nested("c"));
        testItem.listNested = Collections.singletonList(nested);

        db.openNamespace("items", NamespaceOptions.defaultOptions(), TestItem.class);
        db.insert("items", testItem);

        TestItem selectItem = db.query("items", TestItem.class)
                .where("id", Query.Condition.EQ, 1)
                .getOne();

        assertThat(selectItem.equals(testItem), is(true));
    }

    @Test
    public void testUpdateField() {
        TestItem testItem = new TestItem();
        testItem.id = 1;
        testItem.testValue = "test_value";
        testItem.listIntegers = Arrays.asList(1, 2, 3);
        Nested nested = new Nested("a");
        nested.nested = new Nested("b");
        nested.nestedList = Arrays.asList(new Nested("a"), new Nested("b"), new Nested("c"));
        testItem.listNested = Collections.singletonList(nested);

        db.openNamespace("items", NamespaceOptions.defaultOptions(), TestItem.class);
        db.insert("items", testItem);

        db.query("items", TestItem.class)
                .where("id", Query.Condition.EQ, 1)
                .set("t_v", "updated")
                .update();

        TestItem updateItem = db.query("items", TestItem.class)
                .where("id", Query.Condition.EQ, 1)
                .getOne();

        assertThat(updateItem.testValue, is("updated"));
    }

    public static class TestItem {
        @Reindex(name = "id", isPrimaryKey = true)
        @Json("i")
        private Integer id;
        @Json("t_v")
        private String testValue;
        @Json("l_i")
        private List<Integer> listIntegers;
        @Json("l_n")
        private List<Nested> listNested;

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getTestValue() {
            return testValue;
        }

        public void setTestValue(String testValue) {
            this.testValue = testValue;
        }

        public List<Integer> getListIntegers() {
            return listIntegers;
        }

        public void setListIntegers(List<Integer> listIntegers) {
            this.listIntegers = listIntegers;
        }

        public List<Nested> getListNested() {
            return listNested;
        }

        public void setListNested(List<Nested> listNested) {
            this.listNested = listNested;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestItem testItem = (TestItem) o;
            return Objects.equals(id, testItem.id)
                   && Objects.equals(testValue, testItem.testValue)
                    && Objects.equals(listIntegers, testItem.listIntegers)
                   && Objects.equals(listNested, testItem.listNested);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, testValue, listIntegers, listNested);
        }
    }

    public static class Nested {
        @Json("v")
        private String value;
        @Json("n")
        private Nested nested;
        @Json("n_l")
        private List<Nested> nestedList = new ArrayList<>();

        public Nested(String value) {
            this.value = value;
        }

        public Nested() {
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public Nested getNested() {
            return nested;
        }

        public void setNested(Nested nested) {
            this.nested = nested;
        }

        public List<Nested> getNestedList() {
            return nestedList;
        }

        public void setNestedList(List<Nested> nestedList) {
            this.nestedList = nestedList;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Nested nested1 = (Nested) o;
            return Objects.equals(value, nested1.value)
                   && Objects.equals(nested, nested1.nested)
                   && Objects.equals(nestedList, nested1.nestedList);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value, nested, nestedList);
        }
    }

    private void post(String path, Object body) {
        HttpPost httpPost = new HttpPost("http://localhost:9088/api/v1" + path);

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

    private JsonElement getItems() {
        HttpGet httpGet = new HttpGet("http://localhost:9088/api/v1/db/items/namespaces/items/items");

        try (CloseableHttpClient client = HttpClients.createDefault();
             CloseableHttpResponse response = client.execute(httpGet)) {
            InputStream content = response.getEntity().getContent();
            Gson gson = new Gson();
            return gson.fromJson(new InputStreamReader(content), JsonElement.class);
        } catch (IOException e) {
            throw new RuntimeException(e.getLocalizedMessage(), e);
        }
    }

}
