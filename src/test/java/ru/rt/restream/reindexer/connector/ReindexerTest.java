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
import lombok.Getter;
import lombok.Setter;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import ru.rt.restream.reindexer.annotations.Reindex;
import ru.rt.restream.reindexer.connector.options.NamespaceOptions;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

@Testcontainers
public class ReindexerTest {

    @Container
    public GenericContainer<?> reindexer = new GenericContainer<>(DockerImageName.parse("reindexer/reindexer:v2.14.1"))
            .withExposedPorts(9088, 6534);

    private Reindexer db;

    @BeforeEach
    public void setUp() {
        CreateDatabase createDatabase = new CreateDatabase();
        createDatabase.setName("test_items");

        post("/db", createDatabase);

        Integer mappedPort = reindexer.getMappedPort(6534);
        String port = String.valueOf(mappedPort);
        this.db = Configuration.builder()
                .url("cproto://" + "localhost:" + port + "/test_items")
                .getReindexer();
    }

    @Test
    public void testOpenNamespace() {
        String namespaceName = "items";

        db.openNamespace(namespaceName, NamespaceOptions.DEFAULT, TestItem.class);

        NamespaceResponse namespaceResponse = get("/db/test_items/namespaces/items", NamespaceResponse.class);
        MatcherAssert.assertThat(namespaceResponse.name, Matchers.is(namespaceName));
        MatcherAssert.assertThat(namespaceResponse.indexes.size(), Matchers.is(2));
        List<NamespaceResponse.IndexResponse> indexes = namespaceResponse.indexes;
        NamespaceResponse.IndexResponse idIdx = indexes.get(0);
        MatcherAssert.assertThat(idIdx.isPk, Matchers.is(true));
        MatcherAssert.assertThat(idIdx.name, Matchers.is("id"));
        MatcherAssert.assertThat(idIdx.fieldType, Matchers.is("int"));
        NamespaceResponse.IndexResponse nameIdx = indexes.get(1);
        MatcherAssert.assertThat(nameIdx.isPk, Matchers.is(false));
        MatcherAssert.assertThat(nameIdx.name, Matchers.is("name"));
        MatcherAssert.assertThat(nameIdx.fieldType, Matchers.is("string"));
    }

    @Test
    public void testUpsertItem() {
        String namespaceName = "items";
        db.openNamespace(namespaceName, NamespaceOptions.DEFAULT, TestItem.class);

        TestItem testItem = new TestItem();
        testItem.setId(123);
        testItem.setName("TestName");

        db.upsert(namespaceName, testItem);

        ItemsResponse itemsResponse = get("/db/test_items/namespaces/items/items", ItemsResponse.class);
        MatcherAssert.assertThat(itemsResponse.totalItems, Matchers.is(1));
        TestItem responseItem = itemsResponse.items.get(0);
        MatcherAssert.assertThat(responseItem.name, Matchers.is(testItem.name));
        MatcherAssert.assertThat(responseItem.id, Matchers.is(testItem.id));
    }

    private void post(String path, Object body) {
        Integer mappedPort = reindexer.getMappedPort(9088);
        String port = String.valueOf(mappedPort);

        HttpPost httpPost = new HttpPost("http://localhost:" + port + "/api/v1" + path);


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

    private <T> T get(String path, Class<T> clazz) {
        Integer mappedPort = reindexer.getMappedPort(9088);
        String port = String.valueOf(mappedPort);

        HttpGet httpGet = new HttpGet("http://localhost:" + port + "/api/v1" + path);

        try (CloseableHttpClient client = HttpClients.createDefault();
             CloseableHttpResponse response = client.execute(httpGet)) {
            InputStream content = response.getEntity().getContent();
            Gson gson = new GsonBuilder()
                    .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                    .create();
            return gson.fromJson(new InputStreamReader(content), clazz);
        } catch (IOException e) {
            throw new RuntimeException(e.getLocalizedMessage(), e);
        }
    }

    @Getter
    @Setter
    public static class CreateDatabase {

        private String name;

    }

    @Getter
    @Setter
    public static class TestItem {
        @Reindex("id,,pk")
        private Integer id;
        @Reindex("name")
        private String name;
    }

    @Getter
    @Setter
    public static class ItemsResponse {
        private int totalItems;
        private List<TestItem> items;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @Getter
    @Setter
    private static class NamespaceResponse {
        private String name;
        private StorageResponse storage;
        private List<IndexResponse> indexes;

        @JsonIgnoreProperties(ignoreUnknown = true)
        @Getter
        @Setter
        private static class StorageResponse {
            private boolean enabled;
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        @Getter
        @Setter
        private static class IndexResponse {
            private String name;
            private List<String> jsonPaths;
            private String fieldType;
            private String indexType;
            private boolean isPk;
            private boolean isArray;
            private boolean isDense;
            private boolean isSparse;
            private boolean isLinear;
            private boolean isSimpleTag;
            private String collateMode;
            private String sortOrderLetters;
        }
    }

}