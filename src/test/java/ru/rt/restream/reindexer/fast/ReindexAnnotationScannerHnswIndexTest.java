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

package ru.rt.restream.reindexer.fast;

import org.junit.jupiter.api.Test;
import ru.rt.restream.reindexer.FieldType;
import ru.rt.restream.reindexer.ReindexScanner;
import ru.rt.restream.reindexer.ReindexerIndex;
import ru.rt.restream.reindexer.annotations.Hnsw;
import ru.rt.restream.reindexer.annotations.Ivf;
import ru.rt.restream.reindexer.annotations.Metric;
import ru.rt.restream.reindexer.annotations.Reindex;
import ru.rt.restream.reindexer.annotations.ReindexAnnotationScanner;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static ru.rt.restream.reindexer.IndexType.HNSW;
import static ru.rt.restream.reindexer.IndexType.TEXT;
import static ru.rt.restream.util.ReindexerUtils.getIndexByName;

public class ReindexAnnotationScannerHnswIndexTest {
    private final ReindexScanner scanner = new ReindexAnnotationScanner();

    @Test
    public void testConfigDefaultValues_isOk() {
        List<ReindexerIndex> indexes = scanner.parseIndexes(ItemWithDefaultConfigValues.class);
        ReindexerIndex vector = getIndexByName(indexes, "hnsw_vector");

        assertThat(indexes.size(), is(2));
        assertThat(vector, notNullValue());
        assertThat(vector.getIndexType(), is(HNSW));
        assertThat(vector.getHnswConfig(), notNullValue());
        assertThat(vector.getFieldType(), is(FieldType.FLOAT_VECTOR));
        assertFalse(vector.isArray());

        // required
        assertThat(vector.getHnswConfig().getMetric(), is("l2"));
        assertThat(vector.getHnswConfig().getDimension(), is(8));

        // default
        assertThat(vector.getHnswConfig().getStartSize(), is(1000));
        assertThat(vector.getHnswConfig().getM(), is(16));
        assertThat(vector.getHnswConfig().getEfConstruction(), is(200));
        assertThat(vector.getHnswConfig().getMultithreading(), is(0));
    }

    @Test
    public void testConfigExplicitlySetValues_isOk() {
        List<ReindexerIndex> indexes = scanner.parseIndexes(ItemWithAllConfigValues.class);
        ReindexerIndex vector = getIndexByName(indexes, "hnsw_vector");

        assertThat(indexes.size(), is(2));
        assertThat(vector, notNullValue());
        assertThat(vector.getIndexType(), is(HNSW));
        assertThat(vector.getHnswConfig(), notNullValue());
        assertFalse(vector.isArray());

        // required
        assertThat(vector.getHnswConfig().getMetric(), is("cosine"));
        assertThat(vector.getHnswConfig().getDimension(), is(7));

        // default
        assertThat(vector.getHnswConfig().getStartSize(), is(6));
        assertThat(vector.getHnswConfig().getM(), is(5));
        assertThat(vector.getHnswConfig().getEfConstruction(), is(4));
        assertThat(vector.getHnswConfig().getMultithreading(), is(1));
    }

    @Test
    public void testNoHnswIndex_ignoreHnswAnnotation() {
        List<ReindexerIndex> indexes = scanner.parseIndexes(ItemWithNotHnswIndex.class);
        ReindexerIndex vector = getIndexByName(indexes, "txt_float_vector");
        assertThat(vector, notNullValue());
        assertThat(vector.getIndexType(), is(TEXT));
        assertThat(vector.getHnswConfig(), nullValue());
        assertThat(vector.getFieldType(), is(FieldType.FLOAT));
    }

    @Test
    public void testNotArrayField_throwsException() {
        RuntimeException thrown = assertThrows(
                RuntimeException.class,
                () -> scanner.parseIndexes(ItemWithNotArrayField.class),
                "Expected RuntimeException() to throw, but it didn't"
        );
        assertThat(thrown.getMessage(), is("Only a float array field can have vector index"));
    }

    @Test
    public void testNotFloatArrayField_throwsException() {
        RuntimeException thrown = assertThrows(
                RuntimeException.class,
                () -> scanner.parseIndexes(ItemWithNotFloatArrayField.class),
                "Expected RuntimeException() to throw, but it didn't"
        );
        assertThat(thrown.getMessage(), is("Only a float array field can have vector index"));
    }

    @Test
    public void testNoHnswAnnotation_throwsException() {
        RuntimeException thrown = assertThrows(
                RuntimeException.class,
                () -> scanner.parseIndexes(ItemWithoutHnswAnnotation.class),
                "Expected RuntimeException() to throw, but it didn't"
        );
        assertThat(thrown.getMessage(), is("Vector index HNSW must have annotation @Hnsw"));
    }

    static class ItemWithDefaultConfigValues {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "hnsw_vector")
        @Hnsw(metric = Metric.L2, dimension = 8)
        private float[] vector;
    }

    static class ItemWithAllConfigValues {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "hnsw_vector", type = HNSW)
        @Hnsw(metric = Metric.COSINE, dimension = 7, startSize = 6, m = 5, efConstruction = 4, multithreading = true)
        private float[] vector;
    }

    static class ItemWithNotHnswIndex {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "txt_float_vector", type = TEXT)
        @Hnsw(metric = Metric.L2, dimension = 8)
        private float[] vector;
    }

    static class ItemWithNotArrayField {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "hnsw_vector", type = HNSW)
        @Hnsw(metric = Metric.L2, dimension = 8)
        private float vector;
    }

    static class ItemWithNotFloatArrayField {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "hnsw_vector", type = HNSW)
        @Hnsw(metric = Metric.L2, dimension = 8)
        private String[] vector;
    }

    static class ItemWithoutHnswAnnotation {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "hnsw_vector", type = HNSW)
        @Ivf(metric = Metric.L2, dimension = 8)
        private float[] vector;
    }

}
