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
import ru.rt.restream.reindexer.annotations.VecBf;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static ru.rt.restream.reindexer.IndexType.IVF;
import static ru.rt.restream.reindexer.IndexType.TEXT;
import static ru.rt.restream.util.ReindexerUtils.getIndexByName;

public class ReindexAnnotationScannerIvfIndexTest {
    private final ReindexScanner scanner = new ReindexAnnotationScanner();

    @Test
    public void testConfigDefaultValues_isOk() {
        List<ReindexerIndex> indexes = scanner.parseIndexes(ItemWithDefaultConfigValues.class);
        ReindexerIndex vector = getIndexByName(indexes, "ivf_vector");

        assertThat(indexes.size(), is(2));
        assertThat(vector, notNullValue());
        assertThat(vector.getIndexType(), is(IVF));
        assertThat(vector.getIvfConfig(), notNullValue());
        assertThat(vector.getFieldType(), is(FieldType.FLOAT_VECTOR));
        assertFalse(vector.isArray());

        // required
        assertThat(vector.getIvfConfig().getMetric(), is("l2"));
        assertThat(vector.getIvfConfig().getDimension(), is(8));

        // default
        assertThat(vector.getIvfConfig().getCentroidsCount(), is(16));
    }

    @Test
    public void testConfigExplicitlySetValues_isOk() {
        List<ReindexerIndex> indexes = scanner.parseIndexes(ItemWithAllConfigValues.class);
        ReindexerIndex vector = getIndexByName(indexes, "ivf_vector");

        assertThat(indexes.size(), is(2));
        assertThat(vector, notNullValue());
        assertThat(vector.getIndexType(), is(IVF));
        assertThat(vector.getIvfConfig(), notNullValue());
        assertFalse(vector.isArray());

        // required
        assertThat(vector.getIvfConfig().getMetric(), is("cosine"));
        assertThat(vector.getIvfConfig().getDimension(), is(7));

        // default
        assertThat(vector.getIvfConfig().getCentroidsCount(), is(15));
    }

    @Test
    public void testNoIvfIndex_ignoreIvfAnnotation() {
        List<ReindexerIndex> indexes = scanner.parseIndexes(ItemWithNotIvfIndex.class);
        ReindexerIndex vector = getIndexByName(indexes, "txt_float_vector");
        assertThat(vector, notNullValue());
        assertThat(vector.getIndexType(), is(TEXT));
        assertThat(vector.getIvfConfig(), nullValue());
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
    public void testFloatListField_throwsException() {
        RuntimeException thrown = assertThrows(
                RuntimeException.class,
                () -> scanner.parseIndexes(ItemWithFloatListField.class),
                "Expected RuntimeException() to throw, but it didn't"
        );
        assertThat(thrown.getMessage(), is("Only a float array field can have vector index"));
    }

    @Test
    public void testNoIvfAnnotation_throwsException() {
        RuntimeException thrown = assertThrows(
                RuntimeException.class,
                () -> scanner.parseIndexes(ItemWithoutIvfAnnotation.class),
                "Expected RuntimeException() to throw, but it didn't"
        );
        assertThat(thrown.getMessage(), is("Vector index IVF must have annotation @Ivf"));
    }

    static class ItemWithDefaultConfigValues {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "ivf_vector")
        @Ivf(metric = Metric.L2, dimension = 8)
        private float[] vector;
    }

    static class ItemWithAllConfigValues {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "ivf_vector", type = IVF)
        @Ivf(metric = Metric.COSINE, dimension = 7, centroidsCount = 15)
        private float[] vector;
    }

    static class ItemWithNotIvfIndex {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "txt_float_vector", type = TEXT)
        @Ivf(metric = Metric.L2, dimension = 8)
        private float[] vector;
    }

    static class ItemWithNotArrayField {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "ivf_vector", type = IVF)
        @Ivf(metric = Metric.L2, dimension = 8)
        private float vector;
    }

    static class ItemWithNotFloatArrayField {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "ivf_vector", type = IVF)
        @Ivf(metric = Metric.L2, dimension = 8)
        private String[] vector;
    }

    static class ItemWithFloatListField {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "ivf_vector", type = IVF)
        @Ivf(metric = Metric.L2, dimension = 8)
        private List<Float> vector;
    }

    static class ItemWithoutIvfAnnotation {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "ivf_vector", type = IVF)
        @Hnsw(metric = Metric.L2, dimension = 8)
        @VecBf(metric = Metric.L2, dimension = 8)
        private float[] vector;
    }

}
