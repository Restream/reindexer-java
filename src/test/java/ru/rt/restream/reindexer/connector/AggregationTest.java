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

import org.junit.jupiter.api.Test;
import ru.rt.restream.reindexer.CloseableIterator;
import ru.rt.restream.reindexer.Namespace;
import ru.rt.restream.reindexer.Query;
import ru.rt.restream.reindexer.annotations.Reindex;
import ru.rt.restream.reindexer.binding.AggregationResult;
import ru.rt.restream.reindexer.binding.option.NamespaceOptions;
import ru.rt.restream.reindexer.db.DbBaseTest;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Base Aggregation Test.
 */
public abstract class AggregationTest extends DbBaseTest {

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

}
