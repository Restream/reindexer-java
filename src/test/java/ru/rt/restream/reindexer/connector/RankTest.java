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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.rt.restream.reindexer.Namespace;
import ru.rt.restream.reindexer.NamespaceOptions;
import ru.rt.restream.reindexer.ResultIterator;
import ru.rt.restream.reindexer.annotations.Hnsw;
import ru.rt.restream.reindexer.annotations.Metric;
import ru.rt.restream.reindexer.annotations.Reindex;
import ru.rt.restream.reindexer.db.DbBaseTest;
import ru.rt.restream.reindexer.exceptions.ReindexerException;
import ru.rt.restream.reindexer.util.Pair;
import ru.rt.restream.reindexer.vector.params.KnnParams;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static ru.rt.restream.reindexer.Query.Condition.EQ;
import static ru.rt.restream.reindexer.binding.Consts.EMPTY_RANK;

/**
 * Base float vector rank test.
 */
public abstract class RankTest extends DbBaseTest {

    private final String namespaceName = "items";
    private Namespace<VectorItem> vectorNs;

    @BeforeEach
    public void setUp() {
        vectorNs = db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), VectorItem.class);
        List<VectorItem> testItems = getTestVectorItems();
        for (VectorItem item : testItems) {
            db.insert(namespaceName, item);
        }
    }


    @Test
    public void testWithRankAndNoWhereKnn_throwsException() {
        assertThrows(ReindexerException.class,
                () -> db.query(namespaceName, VectorItem.class)
                        .withRank()
                        .where("id", EQ, 1)
                        .execute());
    }

    @Test
    public void testGetItemsWithRank_isOk() {
        ResultIterator<VectorItem> iterator = db.query(namespaceName, VectorItem.class)
                .withRank()
                .whereKnn("vector", new float[]{0.13f, 0.13f, 0.13f, 0.13f, 0.13f, 0.13f, 0.13f, 0.13f},
                        KnnParams.base(5))
                .execute();

        int count = 0;
        while (iterator.hasNext()) {
            VectorItem item = iterator.next();
            float rank = iterator.getCurrentRank();
            assertThat(rank, is(not(EMPTY_RANK)));
            count++;
        }
        assertThat(count, is(5));
    }

    @Test
    public void testGetItemWithoutWhereKnn_returnsMinusOne() {
        ResultIterator<VectorItem> iterator = db.query(namespaceName, VectorItem.class)
                .where("id", EQ, 1)
                .execute();

        VectorItem item = iterator.next();
        float rank = iterator.getCurrentRank();
        assertThat(rank, is(EMPTY_RANK));
    }

    @Test
    public void testGetItemWithWhereKnnAndWithoutRank_returnsRank() {
        ResultIterator<VectorItem> iterator = db.query(namespaceName, VectorItem.class)
//                .withRank()
                .whereKnn("vector", new float[]{0.13f, 0.13f, 0.13f, 0.13f, 0.13f, 0.13f, 0.13f, 0.13f},
                        KnnParams.base(1))
                .execute();

        VectorItem item = iterator.next();
        float rank = iterator.getCurrentRank();
        assertThat(rank, is(not(EMPTY_RANK)));
    }

    @Test
    public void testGetAllItemsWithRank_isOk() {
        Pair<List<VectorItem>, float[]> result = db.query(namespaceName, VectorItem.class)
                .selectAllFields()
                .whereKnn("vector", new float[]{0.13f, 0.13f, 0.13f, 0.13f, 0.13f, 0.13f, 0.13f, 0.13f},
                        KnnParams.base(5))
                .executeAllWithRank();
        List<VectorItem> items = result.getFirst();
        float[] ranks = result.getSecond();

        assertThat(items.size(), is(5));
        assertThat(ranks.length, is(5));
        for (float rank : ranks) {
            assertThat(rank, is(not(EMPTY_RANK)));
        }
    }

    private static List<VectorItem> getTestVectorItems() {
        return Arrays.asList(
                new VectorItem(0, new float[]{0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f}),
                new VectorItem(1, new float[]{0.1f, 0.1f, 0.1f, 0.1f, 0.1f, 0.1f, 0.1f, 0.1f}),
                new VectorItem(2, new float[]{0.2f, 0.2f, 0.2f, 0.2f, 0.2f, 0.2f, 0.2f, 0.2f}),
                new VectorItem(3, new float[]{0.3f, 0.3f, 0.3f, 0.3f, 0.3f, 0.3f, 0.3f, 0.3f}),
                new VectorItem(4, new float[]{0.4f, 0.4f, 0.4f, 0.4f, 0.4f, 0.4f, 0.4f, 0.4f}),
                new VectorItem(5, new float[]{0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f}),
                new VectorItem(6, new float[]{0.6f, 0.6f, 0.6f, 0.6f, 0.6f, 0.6f, 0.6f, 0.6f}),
                new VectorItem(7, new float[]{0.7f, 0.7f, 0.7f, 0.7f, 0.7f, 0.7f, 0.7f, 0.7f}),
                new VectorItem(8, new float[]{0.8f, 0.8f, 0.8f, 0.8f, 0.8f, 0.8f, 0.8f, 0.8f}),
                new VectorItem(9, new float[]{0.9f, 0.9f, 0.9f, 0.9f, 0.9f, 0.9f, 0.9f, 0.9f}),

                new VectorItem(10, new float[]{0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f}),
                new VectorItem(11, new float[]{0.8f, 0.7f, 0.6f, 0.5f, 0.4f, 0.3f, 0.2f, 0.1f}),

                new VectorItem(12, new float[]{0.1f, 0.9f, 0.9f, 0.9f, 0.9f, 0.9f, 0.9f, 0.9f}),
                new VectorItem(13, new float[]{0.1f, 0.1f, 0.9f, 0.9f, 0.9f, 0.9f, 0.9f, 0.9f}),
                new VectorItem(14, new float[]{0.1f, 0.1f, 0.1f, 0.9f, 0.9f, 0.9f, 0.9f, 0.9f}),
                new VectorItem(15, new float[]{0.1f, 0.1f, 0.1f, 0.1f, 0.9f, 0.9f, 0.9f, 0.9f}),
                new VectorItem(16, new float[]{0.1f, 0.1f, 0.1f, 0.1f, 0.1f, 0.9f, 0.9f, 0.9f}),
                new VectorItem(17, new float[]{0.1f, 0.1f, 0.1f, 0.1f, 0.1f, 0.1f, 0.9f, 0.9f}),
                new VectorItem(18, new float[]{0.1f, 0.1f, 0.1f, 0.1f, 0.1f, 0.1f, 0.1f, 0.9f})
        );
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VectorItem {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "vector")
        @Hnsw(metric = Metric.L2, dimension = 8)
        private float[] vector;
    }

}
