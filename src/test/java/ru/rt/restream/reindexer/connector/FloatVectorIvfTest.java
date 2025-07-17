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
import ru.rt.restream.reindexer.annotations.Ivf;
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
import static ru.rt.restream.reindexer.vector.params.KnnParams.radius;
import static ru.rt.restream.util.ReindexerUtils.getIndexByName;

/**
 * Base float vector IVF test.
 */
public abstract class FloatVectorIvfTest extends DbBaseTest {

    private final String namespaceName = "items";
    private Namespace<VectorItem> vectorNs;
    private List<VectorItem> testItems;

    @BeforeEach
    public void setUp() {
        vectorNs = db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), VectorItem.class);
        testItems = getTestVectorItems();
        for (VectorItem item : testItems) {
            db.insert(namespaceName, item);
        }
    }

    @Test
    public void testClassWithIvfAnnotation_hasIvfConfig() {
        ReindexerIndex index = getIndexByName(vectorNs, "vector");

        assertThat(index.getIndexType(), is(IndexType.IVF));
        assertThat(index.getIvfConfig(), notNullValue());
    }

    @Test
    public void testInsertAndGetItemWithIvfIndex_isOk() {
        VectorItem testItem = new VectorItem(111, new float[]{0, 1, 0});
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
    public void testSearchWithBaseParamK_isOk() {
        List<VectorItem> list = db.query(namespaceName, VectorItem.class)
                .selectAllFields()
                .whereKnn("vector", new float[]{0.1f, 0.1f, 0.1f},
                        KnnParams.k(2))
                .toList();

        assertThat(list.size(), is(2));
        assertThat(list.get(0).getId(), is(18));
        assertThat(list.get(0).getVector(), is(testItems.get(18).getVector()));
        assertThat(list.get(1).getId(), is(7));
        assertThat(list.get(1).getVector(), is(testItems.get(7).getVector()));
    }

    @Test
    public void testSearchWithBaseParamRadius_isOk() {
        List<VectorItem> list = db.query(namespaceName, VectorItem.class)
                .selectAllFields()
                .whereKnn("vector", new float[]{0.1f, 0.1f, 0.1f},
                        radius(0.1f))
                .toList();

        assertThat(list.size(), is(4));
        assertThat(list.get(0).getId(), is(18));
        assertThat(list.get(1).getId(), is(6));
        assertThat(list.get(2).getId(), is(7));
        assertThat(list.get(3).getId(), is(8));
    }

    @Test
    public void testSearchWithBaseParamsKAndRadius_isOk() {
        // by k (3 records) + by radius (4 records) = 3 records
        List<VectorItem> list = db.query(namespaceName, VectorItem.class)
                .selectAllFields()
                .whereKnn("vector", new float[]{0.1f, 0.1f, 0.1f},
                        KnnParams.base(3, 0.1f))
                .toList();

        assertThat(list.size(), is(3));
        assertThat(list.get(0).getId(), is(18));
        assertThat(list.get(1).getId(), is(7));
        assertThat(list.get(2).getId(), is(8));

        // by k (5 records) + by radius (4 records) = 4 records
        list = db.query(namespaceName, VectorItem.class)
                .selectAllFields()
                .whereKnn("vector", new float[]{0.1f, 0.1f, 0.1f},
                        KnnParams.base(5, 0.1f))
                .toList();

        assertThat(list.size(), is(4));
        assertThat(list.get(0).getId(), is(18));
        assertThat(list.get(1).getId(), is(6));
        assertThat(list.get(2).getId(), is(7));
        assertThat(list.get(3).getId(), is(8));
    }

    @Test
    public void testSearchWithIvfParams_isOk() {
        // only k - 2 records
        List<VectorItem> list = db.query(namespaceName, VectorItem.class)
                .selectAllFields()
                .whereKnn("vector", new float[]{0.23f, 0.23f, 0.0f},
                        KnnParams.ivf(2, 3))
                .toList();

        assertThat(list.size(), is(2));
        assertThat(list.get(0).getId(), is(8));
        assertThat(list.get(0).getVector(), is(testItems.get(8).getVector()));
        assertThat(list.get(1).getId(), is(18));
        assertThat(list.get(1).getVector(), is(testItems.get(18).getVector()));

        // only radius - 3 records
        list = db.query(namespaceName, VectorItem.class)
                .selectAllFields()
                .whereKnn("vector", new float[]{0.23f, 0.23f, 0.0f},
                        KnnParams.ivf(KnnParams.radius(0.3f), 3))
                .toList();

        assertThat(list.size(), is(3));
        assertThat(list.get(0).getId(), is(8));
        assertThat(list.get(1).getId(), is(18));
        assertThat(list.get(2).getId(), is(19));

        // by k (2 records) + by radius (3 records) = 2 records
        list = db.query(namespaceName, VectorItem.class)
                .selectAllFields()
                .whereKnn("vector", new float[]{0.23f, 0.23f, 0.0f},
                        KnnParams.ivf(KnnParams.base(2, 0.3f), 3))
                .toList();

        assertThat(list.size(), is(2));

        // by k (4 records) + by radius (3 records) = 3 records
        list = db.query(namespaceName, VectorItem.class)
                .selectAllFields()
                .whereKnn("vector", new float[]{0.23f, 0.23f, 0.0f},
                        KnnParams.ivf(KnnParams.base(4, 0.3f), 3))
                .toList();

        assertThat(list.size(), is(3));
    }

    @Test
    public void testTrySearchWithIvfParamFromNullBaseParam_isException() {
        assertThrows(NullPointerException.class,
                () -> db.query(namespaceName, VectorItem.class)
                        .selectAllFields()
                        .whereKnn("vector", new float[]{0.5f, 0.0f, 0.6f},
                                KnnParams.bf(null))
                        .toList());
    }

    @Test
    public void testSearchWithIncorrectIvfParams_isException() {
        assertThrows(IllegalArgumentException.class,
                () -> db.query(namespaceName, VectorItem.class)
                        .selectAllFields()
                        .whereKnn("vector", new float[]{0.5f, 0.0f, 0.6f},
                                KnnParams.ivf(0, 3))
                        .toList(),
                "'k' must be greater than 0");

        assertThrows(IllegalArgumentException.class,
                () -> db.query(namespaceName, VectorItem.class)
                        .selectAllFields()
                        .whereKnn("vector", new float[]{0.5f, 0.0f, 0.6f},
                                KnnParams.ivf(2, 0))
                        .toList(),
                "Minimal value of 'nProbe' must be greater than 0");
    }

    @Test
    public void testSearchWithNotIvfNorBaseParams_isException() {
        assertThrows(RuntimeException.class,
                () -> db.query(namespaceName, VectorItem.class)
                        .selectAllFields()
                        .whereKnn("vector", new float[]{0.0f, 0.7f, 0.5f},
                                KnnParams.bf(2))
                        .toList());

        assertThrows(RuntimeException.class,
                () -> db.query(namespaceName, VectorItem.class)
                        .selectAllFields()
                        .whereKnn("vector", new float[]{0.0f, 0.7f, 0.5f},
                                KnnParams.hnsw(2, 3))
                        .toList());
    }

    private static List<VectorItem> getTestVectorItems() {
        return Arrays.asList(
                new VectorItem(0, new float[]{0, 0, 1}),
                new VectorItem(1, new float[]{0, 1, 0}),
                new VectorItem(2, new float[]{1, 0, 0}),
                new VectorItem(3, new float[]{0, 0, -1}),
                new VectorItem(4, new float[]{0, -1, 0}),
                new VectorItem(5, new float[]{-1, 0, 0}),
                new VectorItem(6, new float[]{0, 1, 1}),
                new VectorItem(7, new float[]{1, 0, 1}),
                new VectorItem(8, new float[]{1, 1, 0}),
                new VectorItem(9, new float[]{0, 1, -1}),
                new VectorItem(10, new float[]{1, 0, -1}),
                new VectorItem(11, new float[]{1, -1, 0}),
                new VectorItem(12, new float[]{0, -1, 1}),
                new VectorItem(13, new float[]{-1, 0, 1}),
                new VectorItem(14, new float[]{-1, 1, 0}),
                new VectorItem(15, new float[]{0, -1, -1}),
                new VectorItem(16, new float[]{-1, 0, -1}),
                new VectorItem(17, new float[]{-1, -1, 0}),
                new VectorItem(18, new float[]{1, 1, 1}),
                new VectorItem(19, new float[]{1, 1, -1}),
                new VectorItem(20, new float[]{1, -1, 1}),
                new VectorItem(21, new float[]{-1, 1, 1}),
                new VectorItem(22, new float[]{1, -1, -1}),
                new VectorItem(23, new float[]{-1, 1, -1}),
                new VectorItem(24, new float[]{-1, -1, 1}),
                new VectorItem(25, new float[]{-1, -1, -1})
        );
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VectorItem {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "vector", type = IndexType.IVF)
        @Ivf(metric = Metric.INNER_PRODUCT, dimension = 3)
        private float[] vector;
    }

}
