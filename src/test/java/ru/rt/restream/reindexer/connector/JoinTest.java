package ru.rt.restream.reindexer.connector;

import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.classic.methods.HttpPost;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.impl.classic.HttpClients;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.io.entity.StringEntity;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import ru.rt.restream.reindexer.CloseableIterator;
import ru.rt.restream.reindexer.Configuration;
import ru.rt.restream.reindexer.Reindexer;
import ru.rt.restream.reindexer.annotations.Joined;
import ru.rt.restream.reindexer.annotations.Reindex;
import ru.rt.restream.reindexer.binding.option.NamespaceOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static ru.rt.restream.reindexer.Query.Condition.*;

@Testcontainers
public class JoinTest {

    @Container
    public GenericContainer<?> reindexer = new GenericContainer<>(DockerImageName.parse("reindexer/reindexer:v2.14.1"))
            .withExposedPorts(9088, 6534);

    private Reindexer db;

    private String restApiPort = "9088";
    private String rpcPort = "6534";

    @BeforeEach
    public void setUp() {
        restApiPort = String.valueOf(reindexer.getMappedPort(9088));
        rpcPort = String.valueOf(reindexer.getMappedPort(6534));
        ReindexerTest.CreateDatabase createDatabase = new ReindexerTest.CreateDatabase();
        createDatabase.setName("test_items");
        post("/db", createDatabase);

        this.db = Configuration.builder()
                .url("cproto://" + "localhost:" + rpcPort + "/test_items")
                .connectionPoolSize(1)
                .connectionTimeout(30L)
                .getReindexer();
    }

    @Test
    public void testSelectOneWithInnerJoinWhenJoinExists() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorNames = new ArrayList<>();
        itemWithJoin.actorNames.add("Test");
        itemWithJoin.actorsIds = new ArrayList<>();
        itemWithJoin.actorsIds.add(1);

        Actor actor = new Actor();
        actor.id = 1;
        actor.name = "Test";
        actor.isVisible = true;

        db.upsert("actors", actor);
        db.upsert("items_with_join", itemWithJoin);

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .join(db.query("actors", Actor.class).where("is_visible", EQ, true), "actors")
                .on("actors_ids", SET, "id")
                .execute();

        MatcherAssert.assertThat(items.hasNext(), Matchers.is(true));
    }

    @Test
    public void testSelectOneWithInnerJoinWhenJoinNotExists() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorNames = new ArrayList<>();
        itemWithJoin.actorNames.add("Test");
        itemWithJoin.actorsIds = new ArrayList<>();
        itemWithJoin.actorsIds.add(1);

        Actor actor = new Actor();
        actor.id = 1;
        actor.name = "Test";
        actor.isVisible = true;

        db.upsert("actors", actor);
        db.upsert("items_with_join", itemWithJoin);

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .join(db.query("actors", Actor.class).where("is_visible", EQ, false), "actors")
                .on("actors_ids", SET, "id")
                .execute();

        MatcherAssert.assertThat(items.hasNext(), Matchers.is(false));
    }

    private static class Actor {

        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "name")
        private String name;

        @Reindex(name = "is_visible")
        private Boolean isVisible;

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Boolean getVisible() {
            return isVisible;
        }

        public void setVisible(Boolean visible) {
            isVisible = visible;
        }
    }

    public static class ItemWithJoin {

        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "name")
        private String name;

        @Reindex(name = "actors_ids")
        private List<Integer> actorsIds;

        @Reindex(name = "actors_names")
        private List<String> actorNames;

        @Joined
        private List<Actor> actors;

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<Integer> getActorsIds() {
            return actorsIds;
        }

        public void setActorsIds(List<Integer> actorsIds) {
            this.actorsIds = actorsIds;
        }

        public List<String> getActorNames() {
            return actorNames;
        }

        public void setActorNames(List<String> actorNames) {
            this.actorNames = actorNames;
        }

        public List<Actor> getActors() {
            return actors;
        }

        public void setActors(List<Actor> actors) {
            this.actors = actors;
        }
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
}
