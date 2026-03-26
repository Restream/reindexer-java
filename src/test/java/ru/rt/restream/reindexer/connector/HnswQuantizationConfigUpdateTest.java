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
import org.junit.jupiter.api.Test;
import ru.rt.restream.reindexer.CollateMode;
import ru.rt.restream.reindexer.FieldType;
import ru.rt.restream.reindexer.IndexType;
import ru.rt.restream.reindexer.Namespace;
import ru.rt.restream.reindexer.NamespaceOptions;
import ru.rt.restream.reindexer.ReindexerIndex;
import ru.rt.restream.reindexer.annotations.Hnsw;
import ru.rt.restream.reindexer.annotations.Json;
import ru.rt.restream.reindexer.annotations.Metric;
import ru.rt.restream.reindexer.annotations.Reindex;
import ru.rt.restream.reindexer.db.DbBaseTest;
import ru.rt.restream.reindexer.vector.HnswConfig;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

abstract class HnswQuantizationConfigUpdateTest extends DbBaseTest {
    private final String namespaceName = "hnsw_quantization_config_update";
    private final String vectorIndexName = "vector";

    @Test
    void testChangeQuantizationConfig() {
        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), VectorItem.class);

        NamespaceDescriptionResponse before = getNamespaceDescription(namespaceName);
        IndexResponse vectorIndexBefore = getIndexByName(before.getIndexes(), vectorIndexName);
        QuantizationConfigResponse quantBefore = vectorIndexBefore.getConfig().getQuantizationConfig();

        assertThat(quantBefore.getQuantizationType(), is("scalar_quantization_8_bit"));
        assertThat(quantBefore.getSampleSize(), is(20_000));
        assertThat(quantBefore.getQuantizationThreshold(), is(100_000));
        assertThat(quantBefore.getQuantile(), nullValue());

        ReindexerIndex updIndex = createHnswIndexForUpdate(vectorIndexName);
        HnswConfig hnswConfig = createHnswConfigForUpdate();
        updIndex.setConfig(hnswConfig);
        db.updateIndex(namespaceName, updIndex);

        NamespaceDescriptionResponse after = getNamespaceDescription(namespaceName);
        IndexResponse vectorIndexAfter = getIndexByName(after.getIndexes(), vectorIndexName);
        QuantizationConfigResponse quantAfter = vectorIndexAfter.getConfig().getQuantizationConfig();

        assertThat(quantAfter.getQuantizationType(), is("scalar_quantization_8_bit"));
        assertEquals(0.987f, quantAfter.getQuantile(), 1e-6f);
        assertThat(quantAfter.getSampleSize(), is(12340));
        assertThat(quantAfter.getQuantizationThreshold(), is(43210));
    }

    private NamespaceDescriptionResponse getNamespaceDescription(String nsName) {
        Namespace<NamespaceDescriptionResponse> serviceNamespace = db.openNamespace("#namespaces",
                NamespaceOptions.defaultOptions(), NamespaceDescriptionResponse.class);
        String query = String.format("select * from #namespaces where name = '%s'", nsName);
        Iterator<NamespaceDescriptionResponse> iterator = serviceNamespace.execSql(query);
        if (!iterator.hasNext()) {
            throw new AssertionError("Namespace is not found: " + nsName);
        }
        return iterator.next();
    }

    private IndexResponse getIndexByName(List<IndexResponse> indexes, String indexName) {
        return indexes.stream()
                .filter(i -> indexName.equals(i.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Index is not found: " + indexName));
    }

    private ReindexerIndex createHnswIndexForUpdate(String indexName) {
        // index update requires the same structural info (index type + field type +
        // json path)
        return ReindexerIndex.builder()
                .name(indexName)
                .jsonPaths(Collections.singletonList(indexName))
                .indexType(IndexType.HNSW)
                .fieldType(FieldType.FLOAT_VECTOR)
                .collateMode(CollateMode.NONE)
                .build();
    }

    private HnswConfig createHnswConfigForUpdate() {
        HnswConfig config = newHnswConfig();
        config.setMetric("cosine");
        config.setDimension(1024);
        config.setStartSize(1000);
        config.setM(20);
        config.setEfConstruction(150);
        config.setMultithreading(0);

        HnswConfig.QuantizationConfig quantConfig = newQuantizationConfig();
        quantConfig.setQuantizationType("scalar_quantization_8_bit");
        quantConfig.setQuantile(0.987f);
        quantConfig.setSampleSize(12340);
        quantConfig.setQuantizationThreshold(43210);
        config.setQuantizationConfig(quantConfig);
        return config;
    }

    private static HnswConfig newHnswConfig() {
        try {
            Constructor<HnswConfig> ctor = HnswConfig.class.getDeclaredConstructor();
            ctor.setAccessible(true);
            return ctor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static HnswConfig.QuantizationConfig newQuantizationConfig() {
        try {
            Constructor<HnswConfig.QuantizationConfig> ctor = HnswConfig.QuantizationConfig.class
                    .getDeclaredConstructor();
            ctor.setAccessible(true);
            return ctor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @Getter
    @Setter
    public static class NamespaceDescriptionResponse {
        private String name;
        private List<IndexResponse> indexes;
    }

    @Getter
    @Setter
    public static class IndexResponse {
        private String name;

        @Json("config")
        private IndexConfigResponse config;
    }

    @Getter
    @Setter
    public static class IndexConfigResponse {
        @Json("quantization_config")
        private QuantizationConfigResponse quantizationConfig;
    }

    @Getter
    @Setter
    public static class QuantizationConfigResponse {
        @Json("quantization_type")
        private String quantizationType;

        @Json("quantile")
        private Float quantile;

        @Json("sample_size")
        private Integer sampleSize;

        @Json("quantization_threshold")
        private Integer quantizationThreshold;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VectorItem {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "vector")
        @Hnsw(metric = Metric.COSINE, dimension = 1024, quantizationConfig = @Hnsw.QuantizationConfig(quantizationType = "scalar_quantization_8_bit"))
        private float[] vector;
    }
}
