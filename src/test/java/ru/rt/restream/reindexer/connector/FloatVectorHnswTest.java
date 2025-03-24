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
import ru.rt.restream.reindexer.IndexType;
import ru.rt.restream.reindexer.Namespace;
import ru.rt.restream.reindexer.NamespaceOptions;
import ru.rt.restream.reindexer.ReindexerIndex;
import ru.rt.restream.reindexer.annotations.Hnsw;
import ru.rt.restream.reindexer.annotations.Metric;
import ru.rt.restream.reindexer.annotations.Reindex;
import ru.rt.restream.reindexer.db.DbBaseTest;
import ru.rt.restream.reindexer.exceptions.ReindexerException;
import ru.rt.restream.reindexer.vector.params.KnnParams;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static ru.rt.restream.reindexer.Query.Condition.EQ;
import static ru.rt.restream.util.ReindexerUtils.getIndexByName;

/**
 * Base float vector HNSW test.
 */
public abstract class FloatVectorHnswTest extends DbBaseTest {

    private final String namespaceName = "items";
    private Namespace<VectorItem> vectorNs;

    @BeforeEach
    public void setUp() {
        vectorNs = db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), VectorItem.class);
    }

    @Test
    public void testClassWithHnswAnnotation_hasHnswConfig() {
        ReindexerIndex index = getIndexByName(vectorNs, "vector");

        assertThat(index.getIndexType(), is(IndexType.HNSW));
        assertThat(index.getHnswConfig(), notNullValue());
    }

    @Test
    public void testInsertAndGetItemWithHnswIndex_isOk() {
        VectorItem testItem = new VectorItem(111, new float[]{0, 1, 0, 1, 0, 1, 0, 1});
        db.insert(namespaceName, testItem);

        // You must explicitly specify the vector field by name or use a predicate `selectAllFields()`.
        Iterator<VectorItem> iterator = db.query(namespaceName, VectorItem.class)
                .where("id", EQ, testItem.getId())
                .select("id", "vector")
                .execute();
        VectorItem item = iterator.next();

        assertThat(item, notNullValue());
        assertThat(item.id, is(testItem.getId()));
        assertThat(item.getVector(), is(testItem.getVector()));
    }

    @Test
    public void testInsertWithWrongVectorSize_isException() {
        assertThrows(ReindexerException.class,
                () -> db.insert(namespaceName, new VectorItem(111, new float[]{0})));

        assertThrows(ReindexerException.class,
                () -> db.insert(namespaceName, new VectorItem(222, new float[]{1, 2, 3, 4, 5, 6, 7, 8, 9})));
    }

    @Test
    public void testSearchWithBaseParams_isOk() {
        List<VectorItem> testItems = getTestVectorItems();
        for (VectorItem item : testItems) {
            db.insert(namespaceName, item);
        }

        List<VectorItem> list = db.query(namespaceName, VectorItem.class)
                .selectAllFields()
                .whereKnn("vector", new float[]{0.13f, 0.13f, 0.13f, 0.13f, 0.13f, 0.13f, 0.13f, 0.13f},
                        KnnParams.base(2))
                .toList();

        assertThat(list.size(), is(2));
        assertThat(list.get(0).getId(), is(1));
        assertThat(list.get(0).getVector(), is(testItems.get(1).getVector()));
        assertThat(list.get(1).getId(), is(2));
        assertThat(list.get(1).getVector(), is(testItems.get(2).getVector()));
    }

    @Test
    public void testSearchWithHnswParams_isOk() {
        List<VectorItem> testItems = getTestVectorItems();
        for (VectorItem item : testItems) {
            db.insert(namespaceName, item);
        }

        List<VectorItem> list = db.query(namespaceName, VectorItem.class)
                .selectAllFields()
                .whereKnn("vector", new float[]{0.23f, 0.23f, 0.23f, 0.23f, 0.23f, 0.23f, 0.23f, 0.23f},
                        KnnParams.hnsw(2, 2))
                .toList();

        assertThat(list.size(), is(2));
        assertThat(list.get(0).getId(), is(2));
        assertThat(list.get(0).getVector(), is(testItems.get(2).getVector()));
        assertThat(list.get(1).getId(), is(3));
        assertThat(list.get(1).getVector(), is(testItems.get(3).getVector()));
    }

    @Test
    public void testSearchWithIncorrectHnswParams_isException() {
        List<VectorItem> testItems = getTestVectorItems();
        for (VectorItem item : testItems) {
            db.insert(namespaceName, item);
        }

        assertThrows(IllegalArgumentException.class,
                () -> db.query(namespaceName, VectorItem.class)
                        .selectAllFields()
                        .whereKnn("vector", new float[]{0.23f, 0.23f, 0.23f, 0.23f, 0.23f, 0.23f, 0.23f, 0.23f},
                                KnnParams.hnsw(0, 2))
                        .toList(),
                "'k' must be greater than 0");

        assertThrows(IllegalArgumentException.class,
                () -> db.query(namespaceName, VectorItem.class)
                        .selectAllFields()
                        .whereKnn("vector", new float[]{0.23f, 0.23f, 0.23f, 0.23f, 0.23f, 0.23f, 0.23f, 0.23f},
                                KnnParams.hnsw(2, 1))
                        .toList(),
                "Minimal value of 'ef' must be greater than or equal to 'k'");
    }

    @Test
    public void testSearchWithNotHnswNorBaseParams_isException() {
        List<VectorItem> testItems = getTestVectorItems();
        for (VectorItem item : testItems) {
            db.insert(namespaceName, item);
        }

        assertThrows(RuntimeException.class,
                () -> db.query(namespaceName, VectorItem.class)
                        .selectAllFields()
                        .whereKnn("vector", new float[]{0.23f, 0.23f, 0.23f, 0.23f, 0.23f, 0.23f, 0.23f, 0.23f},
                                KnnParams.ivf(2, 2))
                        .toList());

        assertThrows(RuntimeException.class,
                () -> db.query(namespaceName, VectorItem.class)
                        .selectAllFields()
                        .whereKnn("vector", new float[]{0.23f, 0.23f, 0.23f, 0.23f, 0.23f, 0.23f, 0.23f, 0.23f},
                                KnnParams.bf(2))
                        .toList());
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
