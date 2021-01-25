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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.classic.methods.HttpGet;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.classic.methods.HttpPost;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.HttpClients;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.io.entity.StringEntity;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import ru.rt.restream.reindexer.Configuration;
import ru.rt.restream.reindexer.binding.option.NamespaceOptions;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Tests for Cproto implementation.
 */
@Testcontainers
public class CprotoReindexerTest extends ReindexerTest {

    @Container
    public GenericContainer<?> reindexer = new GenericContainer<>(DockerImageName.parse("reindexer/reindexer:v2.14.1"))
            .withExposedPorts(9088, 6534);

    private String restApiPort = "9088";
    private String rpcPort = "6534";

    @BeforeEach
    public void setUp() {
        restApiPort = String.valueOf(reindexer.getMappedPort(9088));
        rpcPort = String.valueOf(reindexer.getMappedPort(6534));
        CreateDatabase createDatabase = new CreateDatabase();
        createDatabase.setName("test_items");
        post("/db", createDatabase);
        db = Configuration.builder()
                .url("cproto://" + "localhost:" + rpcPort + "/test_items")
                .connectionPoolSize(4)
                .requestTimeout(Duration.ofSeconds(30L))
                .getReindexer();
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

    @AfterEach
    void tearDown() {
        if (db != null) {
            db.close();
        }
    }

    @Test
    public void testOpenNamespace() {
        String namespaceName = "items";

        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        NamespaceResponse namespaceResponse = get("/db/test_items/namespaces/items", NamespaceResponse.class);
        assertThat(namespaceResponse.name, is(namespaceName));
        assertThat(namespaceResponse.indexes.size(), is(9));
        assertThat(namespaceResponse.storage.enabled, is(true));
        List<NamespaceResponse.IndexResponse> indexes = namespaceResponse.indexes;
        NamespaceResponse.IndexResponse idIdx = indexes.get(0);
        assertThat(idIdx.isPk, is(true));
        assertThat(idIdx.name, is("id"));
        assertThat(idIdx.fieldType, is("int"));
        NamespaceResponse.IndexResponse nameIdx = indexes.get(1);
        assertThat(nameIdx.isPk, is(false));
        assertThat(nameIdx.name, is("name"));
        assertThat(nameIdx.fieldType, is("string"));
    }

    private <T> T get(String path, Class<T> clazz) {
        HttpGet httpGet = new HttpGet("http://localhost:" + restApiPort + "/api/v1" + path);
        try (CloseableHttpClient client = HttpClients.createDefault();
             CloseableHttpResponse response = client.execute(httpGet)) {
            InputStream content = response.getEntity().getContent();
            Gson gson = new GsonBuilder()
                    .create();
            return gson.fromJson(new InputStreamReader(content), clazz);
        } catch (IOException e) {
            throw new RuntimeException(e.getLocalizedMessage(), e);
        }
    }

    public static class CreateDatabase {

        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class NamespaceResponse {
        private String name;
        private StorageResponse storage;
        private List<IndexResponse> indexes;

        @JsonIgnoreProperties(ignoreUnknown = true)
        private static class StorageResponse {
            private boolean enabled;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public StorageResponse getStorage() {
            return storage;
        }

        public void setStorage(StorageResponse storage) {
            this.storage = storage;
        }

        public List<IndexResponse> getIndexes() {
            return indexes;
        }

        public void setIndexes(List<IndexResponse> indexes) {
            this.indexes = indexes;
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        private static class IndexResponse {
            private String name;
            @SerializedName("json_paths")
            private List<String> jsonPaths;
            @SerializedName("field_type")
            private String fieldType;
            @SerializedName("index_type")
            private String indexType;
            @SerializedName("is_pk")
            private boolean isPk;
            @SerializedName("is_array")
            private boolean isArray;
            @SerializedName("is_dense")
            private boolean isDense;
            @SerializedName("is_sparse")
            private boolean isSparse;
            @SerializedName("is_linear")
            private boolean isLinear;
            @SerializedName("is_simple_tag")
            private boolean isSimpleTag;
            @SerializedName("collate_mode")
            private String collateMode;
            @SerializedName("sort_order_letters")
            private String sortOrderLetters;

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }

            public List<String> getJsonPaths() {
                return jsonPaths;
            }

            public void setJsonPaths(List<String> jsonPaths) {
                this.jsonPaths = jsonPaths;
            }

            public String getFieldType() {
                return fieldType;
            }

            public void setFieldType(String fieldType) {
                this.fieldType = fieldType;
            }

            public String getIndexType() {
                return indexType;
            }

            public void setIndexType(String indexType) {
                this.indexType = indexType;
            }

            public boolean isPk() {
                return isPk;
            }

            public void setPk(boolean pk) {
                isPk = pk;
            }

            public boolean isArray() {
                return isArray;
            }

            public void setArray(boolean array) {
                isArray = array;
            }

            public boolean isDense() {
                return isDense;
            }

            public void setDense(boolean dense) {
                isDense = dense;
            }

            public boolean isSparse() {
                return isSparse;
            }

            public void setSparse(boolean sparse) {
                isSparse = sparse;
            }

            public boolean isLinear() {
                return isLinear;
            }

            public void setLinear(boolean linear) {
                isLinear = linear;
            }

            public boolean isSimpleTag() {
                return isSimpleTag;
            }

            public void setSimpleTag(boolean simpleTag) {
                isSimpleTag = simpleTag;
            }

            public String getCollateMode() {
                return collateMode;
            }

            public void setCollateMode(String collateMode) {
                this.collateMode = collateMode;
            }

            public String getSortOrderLetters() {
                return sortOrderLetters;
            }

            public void setSortOrderLetters(String sortOrderLetters) {
                this.sortOrderLetters = sortOrderLetters;
            }
        }
    }

}
