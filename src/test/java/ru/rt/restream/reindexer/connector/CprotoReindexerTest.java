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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.Test;
import ru.rt.restream.category.CprotoTest;
import ru.rt.restream.reindexer.Namespace;
import ru.rt.restream.reindexer.NamespaceOptions;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static ru.rt.restream.reindexer.Query.Condition.EQ;

/**
 * Tests for Cproto implementation.
 */
@CprotoTest
public class CprotoReindexerTest extends ReindexerTest {

    @Test
    public void testOpenNamespace() {
        String namespaceName = "items";

        db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

        NamespaceResponse namespaceResponse = get("/db/items/namespaces/items", NamespaceResponse.class);
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
        NamespaceResponse.IndexResponse valueIdx = indexes.get(7);
        assertThat(valueIdx.isPk, is(false));
        assertThat(valueIdx.name, is("value"));
        assertThat(valueIdx.fieldType, is("string"));
        assertThat(valueIdx.isSparse, is(true));
        assertThat(valueIdx.isNoColumn, is(true));
    }

    @Test
    public void testQuerySelectIdAndName() {
        String namespaceName = "items";
        Namespace<TestItem> ns = db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);
        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");
        testItem.setNonIndex("testNonIndex");
        ns.insert(testItem);
        TestItem item = ns.query()
                .select("id", "name")
                .where("id", EQ, 123)
                .getOne();
        assertThat(item.getId(), is(testItem.getId()));
        assertThat(item.getName(), is(testItem.getName()));
        assertThat(item.getNonIndex(), is(nullValue()));
    }

    private <T> T get(String path, Class<T> clazz) {
        HttpGet httpGet = new HttpGet("http://localhost:9088/api/v1" + path);
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

    @Getter
    @Setter
    private static class NamespaceResponse {
        private String name;
        private StorageResponse storage;
        private List<IndexResponse> indexes;

        private static class StorageResponse {
            private boolean enabled;
        }

        @Getter
        @Setter
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
            @SerializedName("is_no_column")
            private boolean isNoColumn;
            @SerializedName("is_linear")
            private boolean isLinear;
            @SerializedName("is_simple_tag")
            private boolean isSimpleTag;
            @SerializedName("collate_mode")
            private String collateMode;
            @SerializedName("sort_order_letters")
            private String sortOrderLetters;
        }
    }
}
