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

import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.classic.methods.HttpPost;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.HttpClients;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.io.entity.StringEntity;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import ru.rt.restream.reindexer.CloseableIterator;
import ru.rt.restream.reindexer.Configuration;
import ru.rt.restream.reindexer.Namespace;
import ru.rt.restream.reindexer.Query;
import ru.rt.restream.reindexer.Reindexer;
import ru.rt.restream.reindexer.annotations.Reindex;
import ru.rt.restream.reindexer.binding.AggregationResult;
import ru.rt.restream.reindexer.binding.option.NamespaceOptions;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@Testcontainers
public class AggregationTest {

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
    public void testDistinct() {
        Namespace<Item> itemNamespace = db.openNamespace("items", NamespaceOptions.defaultOptions(), Item.class);

        for (int i = 0; i < 1000; i++) {
            Item item = new Item();
            item.setId(i);
            item.setName("Name" + i % 4);
            itemNamespace.insert(item);
        }

        CloseableIterator<Item> result = itemNamespace.query()
                .aggregateDistinct("name")
                .execute();
        AggregationResult aggResult = result.aggResults().get(0);

        assertThat(aggResult.getDistincts().size(), is(4));

        Set<String> expectedDistinctNames = new HashSet<>(Arrays.asList("Name0", "Name1", "Name2", "Name3"));
        assertThat(aggResult.getDistincts().containsAll(expectedDistinctNames), is(true));
    }

    @Test
    public void testSum() {
        Namespace<Item> itemNamespace = db.openNamespace("items", NamespaceOptions.defaultOptions(), Item.class);

        for (int i = 0; i < 1000; i++) {
            Item item = new Item();
            item.setId(i);
            item.setPrice(i);
            itemNamespace.insert(item);
        }

        Query<Item> query = itemNamespace.query()
                .aggregateSum("price");
        CloseableIterator<Item> result = query.execute();
        AggregationResult sumResult = result.aggResults().get(0);
        assertThat(sumResult.getValue(), is(499500D));
    }

    @Test
    public void testAvg() {
        Namespace<Item> itemNamespace = db.openNamespace("items", NamespaceOptions.defaultOptions(), Item.class);

        for (int i = 0; i < 1000; i++) {
            Item item = new Item();
            item.setId(i);
            item.setPrice(i);
            itemNamespace.insert(item);
        }

        Query<Item> query = itemNamespace.query()
                .aggregateAvg("price");
        query.aggregateFacet("id", "price").sort("id", true).offset(10).limit(100);
        CloseableIterator<Item> result = query.execute();
        AggregationResult avgResult = result.aggResults().get(0);
        assertThat(avgResult.getValue(), is(499.5D));
    }

    @Test
    public void testMax() {
        Namespace<Item> itemNamespace = db.openNamespace("items", NamespaceOptions.defaultOptions(), Item.class);

        for (int i = 0; i < 1000; i++) {
            Item item = new Item();
            item.setId(i);
            item.setPrice(i);
            itemNamespace.insert(item);
        }

        Query<Item> query = itemNamespace.query()
                .aggregateMax("price");
        CloseableIterator<Item> result = query.execute();
        AggregationResult maxResult = result.aggResults().get(0);
        assertThat(maxResult.getValue(), is(999D));
    }

    @Test
    public void testMin() {
        Namespace<Item> itemNamespace = db.openNamespace("items", NamespaceOptions.defaultOptions(), Item.class);

        for (int i = 0; i < 1000; i++) {
            Item item = new Item();
            item.setId(i);
            item.setPrice(i);
            itemNamespace.insert(item);
        }

        Query<Item> query = itemNamespace.query()
                .aggregateMin("price");
        CloseableIterator<Item> result = query.execute();
        AggregationResult minResult = result.aggResults().get(0);
        assertThat(minResult.getValue(), is(0D));
    }

    @Test
    public void testFacet() {
        Namespace<Item> itemNamespace = db.openNamespace("items", NamespaceOptions.defaultOptions(), Item.class);

        for (int i = 0; i < 1000; i++) {
            Item item = new Item();
            item.setId(i);
            item.setPrice(i % 8);
            item.setName("Name" + i % 4);
            itemNamespace.insert(item);
        }

        Query<Item> query = itemNamespace.query();
        query.aggregateFacet("name", "price").sort("name", true).sort("price", false).limit(100);
        CloseableIterator<Item> result = query.execute();
        AggregationResult aggResult = result.aggResults().get(0);

        List<AggregationResult.Facet> facets = aggResult.getFacets();

        //8 groups
        assertThat(facets.size(), is(8));

        //names in desc order
        List<String> expectedNames = Arrays.asList("Name3", "Name3", "Name2", "Name2", "Name1", "Name1", "Name0",
                "Name0");
        //prices in asc order
        List<String> expectedPrices = Arrays.asList("3", "7", "2", "6", "1", "5", "0", "4");

        for (int i = 0; i < facets.size(); i++) {
            assertThat(facets.get(0).getCount(), is(125));
            AggregationResult.Facet facet = facets.get(i);
            assertThat(facet.getValues().get(0), is(expectedNames.get(i)));
            assertThat(facet.getValues().get(1), is(expectedPrices.get(i)));
            assertThat(facet.getCount(), is(125));
        }
    }

    public static class Item {

        @Reindex(name = "id", isPrimaryKey = true)
        private int id;

        @Reindex(name = "price")
        private int price;

        private String name;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getPrice() {
            return price;
        }

        public void setPrice(int price) {
            this.price = price;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
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

}
