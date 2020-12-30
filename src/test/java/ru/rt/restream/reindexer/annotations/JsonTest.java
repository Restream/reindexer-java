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

import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.classic.methods.HttpGet;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.classic.methods.HttpPost;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.HttpClients;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.io.entity.StringEntity;
import com.google.gson.*;
import com.google.gson.annotations.SerializedName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import ru.rt.restream.reindexer.Configuration;
import ru.rt.restream.reindexer.Query;
import ru.rt.restream.reindexer.Reindexer;
import ru.rt.restream.reindexer.binding.option.NamespaceOptions;
import ru.rt.restream.reindexer.connector.ReindexerTest;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@Testcontainers
class JsonTest {

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
        ReindexerTest.CreateDatabase createDatabase = new ReindexerTest.CreateDatabase();
        createDatabase.setName("test_items");
        post("/db", createDatabase);

        this.db = Configuration.builder()
                .url("cproto://" + "localhost:" + rpcPort + "/test_items")
                .threadPoolSize(1)
                .connectionPoolSize(4)
                .requestTimeout(30L)
                .getReindexer();
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

    private static class TestItem {
        @Reindex(name = "id", isPrimaryKey = true)
        @Json("i")
        private Integer id;
        @Json("t_v")
        private String testValue;
        @Json("l_i")
        private List<Integer> listIntegers;
        @Json("l_n")
        private List<Nested> listNested;

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

    private static class Nested {
        @Json("v")
        private String value;
        @Json("n")
        private Nested nested;
        @Json("n_l")
        private List<Nested> nestedList;

        public Nested(String value) {
            this.value = value;
        }

        public Nested() {
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

    private JsonElement getItems() {
        HttpGet httpGet = new HttpGet("http://localhost:" + restApiPort + "/api/v1/db/test_items/namespaces/items/items");

        try (CloseableHttpClient client = HttpClients.createDefault();
             CloseableHttpResponse response = client.execute(httpGet)) {
            InputStream content = response.getEntity().getContent();
            Gson gson = new Gson();
            return gson.fromJson(new InputStreamReader(content), JsonElement.class);
        } catch (IOException e) {
            throw new RuntimeException(e.getLocalizedMessage(), e);
        }
    }

    public static class ItemsResponse {
        @SerializedName("total_items")
        private int totalItems;
        private List<String> items;
    }

}
