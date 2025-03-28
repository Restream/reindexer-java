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
import static ru.rt.restream.reindexer.IndexType.TEXT;
import static ru.rt.restream.reindexer.IndexType.VEC_BF;
import static ru.rt.restream.util.ReindexerUtils.getIndexByName;

public class ReindexAnnotationScannerVecBfIndexTest {
    private final ReindexScanner scanner = new ReindexAnnotationScanner();

    @Test
    public void testConfigDefaultValues_isOk() {
        List<ReindexerIndex> indexes = scanner.parseIndexes(ItemWithDefaultConfigValues.class);
        ReindexerIndex vector = getIndexByName(indexes, "vec_bf_vector");

        assertThat(indexes.size(), is(2));
        assertThat(vector, notNullValue());
        assertThat(vector.getIndexType(), is(VEC_BF));
        assertThat(vector.getVecBfConfig(), notNullValue());
        assertThat(vector.getFieldType(), is(FieldType.FLOAT_VECTOR));
        assertFalse(vector.isArray());

        // required
        assertThat(vector.getVecBfConfig().getMetric(), is("l2"));
        assertThat(vector.getVecBfConfig().getDimension(), is(8));

        // default
        assertThat(vector.getVecBfConfig().getStartSize(), is(1000));
    }

    @Test
    public void testConfigExplicitlySetValues_isOk() {
        List<ReindexerIndex> indexes = scanner.parseIndexes(ItemWithAllConfigValues.class);
        ReindexerIndex vector = getIndexByName(indexes, "vec_bf_vector");

        assertThat(indexes.size(), is(2));
        assertThat(vector, notNullValue());
        assertThat(vector.getIndexType(), is(VEC_BF));
        assertThat(vector.getVecBfConfig(), notNullValue());
        assertFalse(vector.isArray());

        // required
        assertThat(vector.getVecBfConfig().getMetric(), is("cosine"));
        assertThat(vector.getVecBfConfig().getDimension(), is(7));

        // default
        assertThat(vector.getVecBfConfig().getStartSize(), is(999));
    }

    @Test
    public void testNoVecBfIndex_ignoreVecBfAnnotation() {
        List<ReindexerIndex> indexes = scanner.parseIndexes(ItemWithNotVecBfIndex.class);
        ReindexerIndex vector = getIndexByName(indexes, "txt_float_vector");
        assertThat(vector, notNullValue());
        assertThat(vector.getIndexType(), is(TEXT));
        assertThat(vector.getVecBfConfig(), nullValue());
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
    public void testNoVecBfAnnotation_throwsException() {
        RuntimeException thrown = assertThrows(
                RuntimeException.class,
                () -> scanner.parseIndexes(ItemWithoutVecBfAnnotation.class),
                "Expected RuntimeException() to throw, but it didn't"
        );
        assertThat(thrown.getMessage(), is("Vector index VEC_BF must have annotation @VecBf"));
    }

    static class ItemWithDefaultConfigValues {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "vec_bf_vector")
        @VecBf(metric = Metric.L2, dimension = 8)
        private float[] vector;
    }

    static class ItemWithAllConfigValues {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "vec_bf_vector", type = VEC_BF)
        @VecBf(metric = Metric.COSINE, dimension = 7, startSize = 999)
        private float[] vector;
    }

    static class ItemWithNotVecBfIndex {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "txt_float_vector", type = TEXT)
        @VecBf(metric = Metric.L2, dimension = 8)
        private float[] vector;
    }

    static class ItemWithNotArrayField {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "vec_bf_vector", type = VEC_BF)
        @VecBf(metric = Metric.L2, dimension = 8)
        private float vector;
    }

    static class ItemWithNotFloatArrayField {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "vec_bf_vector", type = VEC_BF)
        @VecBf(metric = Metric.L2, dimension = 8)
        private String[] vector;
    }

    static class ItemWithoutVecBfAnnotation {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "vec_bf_vector", type = VEC_BF)
        @Hnsw(metric = Metric.L2, dimension = 8)
        @Ivf(metric = Metric.L2, dimension = 8)
        private float[] vector;
    }

}
