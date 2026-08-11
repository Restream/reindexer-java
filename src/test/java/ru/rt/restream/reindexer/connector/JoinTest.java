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
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.Test;
import ru.rt.restream.reindexer.NamespaceOptions;
import ru.rt.restream.reindexer.Query;
import ru.rt.restream.reindexer.QueryResultJsonIterator;
import ru.rt.restream.reindexer.ResultIterator;
import ru.rt.restream.reindexer.annotations.Reindex;
import ru.rt.restream.reindexer.annotations.Transient;
import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.db.DbBaseTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static ru.rt.restream.reindexer.Query.Condition.EQ;
import static ru.rt.restream.reindexer.Query.Condition.RANGE;
import static ru.rt.restream.reindexer.Query.Condition.SET;

/**
 * Base Join test.
 */
public abstract class JoinTest extends DbBaseTest {

    @Test
    public void testJoinToManyExists() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorName = "Test";
        itemWithJoin.actorsIds = new ArrayList<>();
        itemWithJoin.actorsIds.add(1);

        Actor actor = new Actor();
        actor.id = 1;
        actor.name = "Test";
        actor.visible = true;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actor);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .join(db.query("actors", Actor.class)
                        .on("actorsIds", Query.Condition.SET, "id"), "joinedActors")
                .execute();

        assertThat(items.hasNext(), is(true));

        ItemWithJoin result = items.next();
        List<Actor> joinedActors = result.getJoinedActors();
        assertThat(joinedActors.size(), is(1));
        Actor resultActor = joinedActors.get(0);
        assertThat(resultActor.getId(), is(actor.id));
        assertThat(resultActor.getName(), is(actor.name));
        assertThat(resultActor.isVisible(), is(actor.visible));
    }

    @Test
    public void testJoinToManyNotExists() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorName = "Test";
        itemWithJoin.actorsIds = new ArrayList<>();
        itemWithJoin.actorsIds.add(2);

        Actor actor = new Actor();
        actor.id = 1;
        actor.name = "Test";
        actor.visible = true;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actor);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .join(db.query("actors", Actor.class)
                        .on("actorsIds", Query.Condition.SET, "id"), "joinedActors")
                .execute();

        assertThat(items.hasNext(), is(false));
    }

    @Test
    public void testJoinToManyWithFilterExists() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorName = "Test";
        itemWithJoin.actorsIds = new ArrayList<>();
        itemWithJoin.actorsIds.add(1);

        Actor actor = new Actor();
        actor.id = 1;
        actor.name = "Test";
        actor.visible = true;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actor);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .join(db.query("actors", Actor.class).where("is_visible", EQ, true)
                        .on("actorsIds", Query.Condition.SET, "id"), "joinedActors")
                .execute();

        assertThat(items.hasNext(), is(true));

        ItemWithJoin result = items.next();
        List<Actor> joinedActors = result.getJoinedActors();
        assertThat(joinedActors.size(), is(1));
        Actor resultActor = joinedActors.get(0);
        assertThat(resultActor.getId(), is(actor.id));
        assertThat(resultActor.getName(), is(actor.name));
        assertThat(resultActor.isVisible(), is(actor.visible));
    }

    @Test
    public void testJoinToManyWithFilterNotExists() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorsIds = new ArrayList<>();
        itemWithJoin.actorsIds.add(1);

        Actor actor = new Actor();
        actor.id = 1;
        actor.name = "Test";
        actor.visible = false;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actor);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .join(db.query("actors", Actor.class).where("is_visible", EQ, true)
                        .on("actorsIds", Query.Condition.SET, "id"), "joinedActors")
                .execute();

        assertThat(items.hasNext(), is(false));
    }

    @Test
    public void testLeftJoinToManyExists() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorsIds = new ArrayList<>();
        itemWithJoin.actorsIds.add(1);

        Actor actor = new Actor();
        actor.id = 1;
        actor.name = "Test";
        actor.visible = true;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actor);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .leftJoin(db.query("actors", Actor.class)
                        .on("actorsIds", Query.Condition.SET, "id"), "joinedActors")
                .execute();

        assertThat(items.hasNext(), is(true));

        ItemWithJoin result = items.next();
        List<Actor> joinedActors = result.getJoinedActors();
        assertThat(joinedActors.size(), is(1));
        Actor resultActor = joinedActors.get(0);
        assertThat(resultActor.getId(), is(actor.id));
        assertThat(resultActor.getName(), is(actor.name));
        assertThat(resultActor.isVisible(), is(actor.visible));
    }

    @Test
    public void testLeftJoinToManyNotExists() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorsIds = new ArrayList<>();
        itemWithJoin.actorsIds.add(2);

        Actor actor = new Actor();
        actor.id = 1;
        actor.name = "Test";
        actor.visible = true;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actor);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .leftJoin(db.query("actors", Actor.class)
                        .on("actorsIds", Query.Condition.SET, "id"), "joinedActors")
                .execute();

        assertThat(items.hasNext(), is(true));

        ItemWithJoin result = items.next();
        List<Actor> joinedActors = result.getJoinedActors();
        assertThat(joinedActors.size(), is(0));
    }

    @Test
    public void testLeftJoinToManyWithFilterExists() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorsIds = new ArrayList<>();
        itemWithJoin.actorsIds.add(1);

        Actor actor = new Actor();
        actor.id = 1;
        actor.name = "Test";
        actor.visible = true;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actor);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .leftJoin(db.query("actors", Actor.class)
                        .where("is_visible", EQ, true)
                        .on("actorsIds", Query.Condition.SET, "id"), "joinedActors")
                .execute();

        assertThat(items.hasNext(), is(true));

        ItemWithJoin result = items.next();
        List<Actor> joinedActors = result.getJoinedActors();
        assertThat(joinedActors.size(), is(1));
        Actor resultActor = joinedActors.get(0);
        assertThat(resultActor.getId(), is(actor.id));
        assertThat(resultActor.getName(), is(actor.name));
        assertThat(resultActor.isVisible(), is(actor.visible));
    }

    @Test
    public void testLeftJoinToManyWithFilterNotExists() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorsIds = new ArrayList<>();
        itemWithJoin.actorsIds.add(1);

        Actor actor = new Actor();
        actor.id = 1;
        actor.name = "Test";
        actor.visible = false;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actor);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .leftJoin(db.query("actors", Actor.class)
                        .where("is_visible", EQ, true)
                        .on("actorsIds", Query.Condition.SET, "id"), "joinedActors")
                .execute();

        assertThat(items.hasNext(), is(true));
        ItemWithJoin result = items.next();
        List<Actor> joinedActors = result.getJoinedActors();
        assertThat(joinedActors.size(), is(0));
    }

    @Test
    public void testJoinToOneExists() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorName = "Test";
        itemWithJoin.actorsIds = Collections.singletonList(1);

        Actor actor = new Actor();
        actor.id = 1;
        actor.name = "Test";
        actor.visible = true;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actor);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .join(db.query("actors", Actor.class)
                        .on("actorName", Query.Condition.EQ, "name"), "joinedActor")
                .execute();

        assertThat(items.hasNext(), is(true));

        ItemWithJoin result = items.next();
        Actor resultActor = result.getJoinedActor();
        assertThat(resultActor.getId(), is(actor.id));
        assertThat(resultActor.getName(), is(actor.name));
        assertThat(resultActor.isVisible(), is(actor.visible));
    }

    @Test
    public void testJoinToOneNotExists() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorName = "Test";
        itemWithJoin.actorsIds = new ArrayList<>();

        Actor actor = new Actor();
        actor.id = 1;
        actor.name = "Test";
        actor.visible = true;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actor);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .join(db.query("actors", Actor.class)
                        .on("actorsIds", Query.Condition.SET, "id"), "joinedActor")
                .execute();

        assertThat(items.hasNext(), is(false));
    }

    @Test
    public void testJoinToOneWithFilterExists() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorName = "Test";

        Actor actor = new Actor();
        actor.id = 1;
        actor.name = "Test";
        actor.visible = true;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actor);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .join(db.query("actors", Actor.class)
                        .where("is_visible", EQ, true)
                        .on("actorName", Query.Condition.SET, "name"), "joinedActor")
                .execute();

        assertThat(items.hasNext(), is(true));
        ItemWithJoin result = items.next();
        Actor resultActor = result.getJoinedActor();
        assertThat(resultActor.getId(), is(actor.id));
        assertThat(resultActor.getName(), is(actor.name));
        assertThat(resultActor.isVisible(), is(actor.visible));
    }

    @Test
    public void testJoinToOneWithFilterNotExists() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorName = "Test";

        Actor actor = new Actor();
        actor.id = 1;
        actor.name = "Test";
        actor.visible = false;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actor);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .join(db.query("actors", Actor.class)
                        .where("is_visible", EQ, true)
                        .on("actorName", Query.Condition.SET, "name"), "joinedActor")
                .execute();

        assertThat(items.hasNext(), is(false));
    }

    @Test
    public void testLeftJoinToOneExists() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorName = "Test";

        Actor actor = new Actor();
        actor.id = 1;
        actor.name = "Test";
        actor.visible = true;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actor);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .leftJoin(db.query("actors", Actor.class)
                        .on("actorName", Query.Condition.SET, "name"), "joinedActor")
                .execute();

        assertThat(items.hasNext(), is(true));

        ItemWithJoin result = items.next();
        Actor resultActor = result.getJoinedActor();
        assertThat(resultActor.getId(), is(actor.id));
        assertThat(resultActor.getName(), is(actor.name));
        assertThat(resultActor.isVisible(), is(actor.visible));
    }

    @Test
    public void testLeftJoinToOneNotExists() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorName = "Test";

        Actor actor = new Actor();
        actor.id = 1;
        actor.name = "NotTest";
        actor.visible = true;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actor);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .leftJoin(db.query("actors", Actor.class)
                        .on("actorName", Query.Condition.SET, "name"), "joinedActor")
                .execute();

        assertThat(items.hasNext(), is(true));

        ItemWithJoin result = items.next();
        Actor resultActor = result.getJoinedActor();
        assertThat(resultActor, is(nullValue()));
    }

    @Test
    public void testLeftJoinToOneWithFilterExists() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorName = "Test";

        Actor actor = new Actor();
        actor.id = 1;
        actor.name = "Test";
        actor.visible = true;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actor);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .leftJoin(db.query("actors", Actor.class)
                        .where("is_visible", EQ, true)
                        .on("actorName", Query.Condition.SET, "name"), "joinedActor")
                .execute();

        assertThat(items.hasNext(), is(true));

        ItemWithJoin result = items.next();
        Actor resultActor = result.getJoinedActor();
        assertThat(resultActor.getId(), is(actor.id));
        assertThat(resultActor.getName(), is(actor.name));
        assertThat(resultActor.isVisible(), is(actor.visible));
    }

    @Test
    public void testLeftJoinToOneWithFilterNotExists() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorName = "Test";

        Actor actor = new Actor();
        actor.id = 1;
        actor.name = "Test";
        actor.visible = false;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actor);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .leftJoin(db.query("actors", Actor.class)
                        .where("is_visible", EQ, true)
                        .on("actorName", Query.Condition.SET, "name"), "joinedActors")
                .execute();

        assertThat(items.hasNext(), is(true));
        ItemWithJoin result = items.next();
        Actor joinedActor = result.getJoinedActor();
        assertThat(joinedActor, is(nullValue()));
    }

    @Test
    public void testMultipleJoinsExists() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorName = "Test";
        itemWithJoin.actorsIds = Collections.singletonList(1);

        Actor actorById = new Actor();
        actorById.id = 1;
        actorById.name = "NotTest";
        actorById.visible = true;

        Actor actorByName = new Actor();
        actorByName.id = 2;
        actorByName.name = "Test";
        actorByName.visible = true;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actorById);
        db.upsert("actors", actorByName);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .join(db.query("actors", Actor.class)
                        .on("actorsIds", Query.Condition.SET, "id"), "joinedActors")
                .join(db.query("actors", Actor.class)
                        .on("actorName", Query.Condition.SET, "name"), "joinedActor")
                .execute();

        assertThat(items.hasNext(), is(true));
        ItemWithJoin result = items.next();

        List<Actor> joinedActors = result.getJoinedActors();
        assertThat(joinedActors.size(), is(1));
        assertThat(joinedActors.get(0).id, is(actorById.id));
        assertThat(joinedActors.get(0).name, is(actorById.name));
        assertThat(joinedActors.get(0).visible, is(actorById.visible));

        Actor actor = result.getJoinedActor();
        assertThat(actor.id, is(actorByName.id));
        assertThat(actor.name, is(actorByName.name));
        assertThat(actor.visible, is(actorByName.visible));
    }

    @Test
    public void testMultipleJoinsNotExists() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorName = "Test";
        itemWithJoin.actorsIds = Collections.singletonList(2);

        Actor actorById = new Actor();
        actorById.id = 1;
        actorById.name = "NotTest";
        actorById.visible = true;

        Actor actorByName = new Actor();
        actorByName.id = 2;
        actorByName.name = "NotTest";
        actorByName.visible = true;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actorById);
        db.upsert("actors", actorByName);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .join(db.query("actors", Actor.class)
                        .on("actorsIds", Query.Condition.SET, "id"), "joinedActors")
                .join(db.query("actors", Actor.class)
                        .on("actorName", Query.Condition.SET, "name"), "joinedActor")
                .execute();

        assertThat(items.hasNext(), is(false));
    }

    @Test
    public void testJoinOnMultipleConditions() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorName = "Test";
        itemWithJoin.actorsIds = Arrays.asList(1, 2);

        Actor actorById = new Actor();
        actorById.id = 1;
        actorById.name = "Test";
        actorById.visible = true;

        Actor actorByName = new Actor();
        actorByName.id = 2;
        actorByName.name = "NotTest";
        actorByName.visible = true;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actorById);
        db.upsert("actors", actorByName);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .join(db.query("actors", Actor.class)
                        .on("actorsIds", Query.Condition.SET, "id")
                        .on("actorName", Query.Condition.SET, "name"), "joinedActors")
                .execute();

        assertThat(items.hasNext(), is(true));
        ItemWithJoin result = items.next();

        List<Actor> joinedActors = result.getJoinedActors();
        assertThat(joinedActors.size(), is(1));
        assertThat(joinedActors.get(0).id, is(actorById.id));
        assertThat(joinedActors.get(0).name, is(actorById.name));
        assertThat(joinedActors.get(0).visible, is(actorById.visible));
    }

    @Test
    public void testJoinOnMultipleConditionsWithOrExists() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorName = "Test";
        itemWithJoin.actorsIds = Collections.singletonList(1);

        Actor actorById = new Actor();
        actorById.id = 1;
        actorById.name = "NotTest";
        actorById.visible = true;

        Actor actorByName = new Actor();
        actorByName.id = 2;
        actorByName.name = "Test";
        actorByName.visible = true;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actorById);
        db.upsert("actors", actorByName);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .join(db.query("actors", Actor.class)
                        .on("actorsIds", Query.Condition.SET, "id")
                        .or()
                        .on("actorName", Query.Condition.SET, "name"), "joinedActors")
                .execute();

        assertThat(items.hasNext(), is(true));
        ItemWithJoin result = items.next();

        List<Actor> joinedActors = result.getJoinedActors();
        assertThat(joinedActors.size(), is(2));
        assertThat(joinedActors.get(0).id, is(actorById.id));
        assertThat(joinedActors.get(0).name, is(actorById.name));
        assertThat(joinedActors.get(0).visible, is(actorById.visible));
        assertThat(joinedActors.get(1).id, is(actorByName.id));
        assertThat(joinedActors.get(1).name, is(actorByName.name));
        assertThat(joinedActors.get(1).visible, is(actorByName.visible));
    }

    @Test
    public void testJoinInWherePart() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin item1 = new ItemWithJoin();
        item1.id = 1;
        item1.name = "resultById";

        ItemWithJoin item2 = new ItemWithJoin();
        item2.id = 101;
        item2.name = "resultByInnerJoinName";
        item2.actorName = "Test";
        item2.actorsIds = Collections.singletonList(103);

        ItemWithJoin item3 = new ItemWithJoin();
        item3.id = 102;
        item3.name = "resultByInnerJoinId";
        item3.actorName = "NotTest";
        item3.actorsIds = Collections.singletonList(201);

        Actor actorByName = new Actor();
        actorByName.id = 103;
        actorByName.name = "Test";

        Actor actorById = new Actor();
        actorById.id = 201;
        actorById.name = "NotTest";

        db.upsert("items_with_join", item1);
        db.upsert("items_with_join", item2);
        db.upsert("items_with_join", item3);
        db.upsert("actors", actorById);
        db.upsert("actors", actorByName);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .where("id", RANGE, 0, 100)
                .or()
                .innerJoin(db.query("actors", Actor.class)
                        .where("name", EQ, "Test")
                        .on("actorsIds", SET, "id"), "joinedActors")
                .or()
                .innerJoin(db.query("actors", Actor.class)
                        .where("id", RANGE, 200, 300)
                        .on("actorsIds", SET, "id"), "joinedActors")
                .execute();

        ItemWithJoin resultById = items.next();
        ItemWithJoin resultByInnerJoinName = items.next();
        ItemWithJoin resultByInnerJoinId = items.next();

        assertThat(resultById.id, is(item1.id));
        assertThat(resultById.name, is(item1.name));
        assertThat(resultById.joinedActors.size(), is(0));

        assertThat(resultByInnerJoinName.id, is(item2.id));
        assertThat(resultByInnerJoinName.name, is(item2.name));
        assertThat(resultByInnerJoinName.joinedActors.size(), is(1));
        assertThat(resultByInnerJoinName.getJoinedActors().get(0).id, is(103));

        assertThat(resultByInnerJoinId.id, is(item3.id));
        assertThat(resultByInnerJoinId.name, is(item3.name));
        assertThat(resultByInnerJoinId.joinedActors.size(), is(1));
        assertThat(resultByInnerJoinId.getJoinedActors().get(0).id, is(201));

    }

    @Test
    public void testMultipleJoinsSameFieldAccumulateResults() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin item = new ItemWithJoin();
        item.id = 1;
        item.name = "item";
        item.actorsIds = Arrays.asList(10, 30);
        db.upsert("items_with_join", item);

        Actor first = new Actor();
        first.id = 10;
        first.name = "first";
        first.visible = true;
        db.upsert("actors", first);

        Actor second = new Actor();
        second.id = 30;
        second.name = "second";
        second.visible = true;
        db.upsert("actors", second);

        Actor unmatched = new Actor();
        unmatched.id = 20;
        unmatched.name = "unmatched";
        unmatched.visible = true;
        db.upsert("actors", unmatched);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .where("id", EQ, 1)
                .leftJoin(db.query("actors", Actor.class)
                        .where("id", EQ, 10)
                        .on("actorsIds", SET, "id"), "joinedActors")
                .leftJoin(db.query("actors", Actor.class)
                        .where("id", EQ, 30)
                        .on("actorsIds", SET, "id"), "joinedActors")
                .leftJoin(db.query("actors", Actor.class)
                        .where("id", EQ, 20)
                        .on("actorsIds", SET, "id"), "joinedActors")
                .execute();

        assertThat(items.hasNext(), is(true));
        ItemWithJoin result = items.next();
        assertThat(result.joinedActors.size(), is(2));
        assertThat(result.joinedActors.get(0).id, is(10));
        assertThat(result.joinedActors.get(0).name, is("first"));
        assertThat(result.joinedActors.get(1).id, is(30));
        assertThat(result.joinedActors.get(1).name, is("second"));
    }

    @Test
    public void testNestedJoin() {
        assumeQueryFormatV2();

        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);
        db.openNamespace("roles", NamespaceOptions.defaultOptions(), Role.class);

        ItemWithJoin item = new ItemWithJoin();
        item.id = 1;
        item.name = "item";
        item.actorsIds = Collections.singletonList(10);
        db.upsert("items_with_join", item);

        Actor actor = new Actor();
        actor.id = 10;
        actor.name = "actor";
        actor.visible = true;
        db.upsert("actors", actor);

        Role role = new Role();
        role.id = 100;
        role.name = "actor";
        db.upsert("roles", role);

        Query<Actor> actorQuery = db.query("actors", Actor.class)
                .innerJoin(db.query("roles", Role.class), "joinedRoles")
                .on("name", EQ, "name");

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .where("id", EQ, 1)
                .innerJoin(actorQuery, "joinedActors")
                .on("actorsIds", SET, "id")
                .execute();

        assertThat(items.hasNext(), is(true));
        ItemWithJoin result = items.next();
        assertThat(result.joinedActors.size(), is(1));
        Actor joinedActor = result.joinedActors.get(0);
        assertThat(joinedActor.id, is(10));
        assertThat(joinedActor.name, is("actor"));
        assertThat(joinedActor.joinedRoles.size(), is(1));
        assertThat(joinedActor.joinedRoles.get(0).id, is(100));
        assertThat(joinedActor.joinedRoles.get(0).name, is("actor"));
    }

    @Test
    public void testNestedJoinWithLeftAndInnerCombinations() {
        assumeQueryFormatV2();

        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);
        db.openNamespace("roles", NamespaceOptions.defaultOptions(), Role.class);

        ItemWithJoin itemWithRole = new ItemWithJoin();
        itemWithRole.id = 1;
        itemWithRole.actorsIds = Collections.singletonList(10);
        db.upsert("items_with_join", itemWithRole);

        ItemWithJoin itemWithoutRole = new ItemWithJoin();
        itemWithoutRole.id = 2;
        itemWithoutRole.actorsIds = Collections.singletonList(20);
        db.upsert("items_with_join", itemWithoutRole);

        Actor actorWithRole = new Actor();
        actorWithRole.id = 10;
        actorWithRole.name = "actor-with-role";
        db.upsert("actors", actorWithRole);

        Actor actorWithoutRole = new Actor();
        actorWithoutRole.id = 20;
        actorWithoutRole.name = "actor-without-role";
        db.upsert("actors", actorWithoutRole);

        Role role = new Role();
        role.id = 100;
        role.name = "actor-with-role";
        db.upsert("roles", role);

        Query<Actor> actorWithInnerRoleQuery = db.query("actors", Actor.class)
                .innerJoin(db.query("roles", Role.class), "joinedRoles")
                .on("name", EQ, "name");

        List<ItemWithJoin> leftWithInnerNestedItems = db.query("items_with_join", ItemWithJoin.class)
                .sort("id", false)
                .leftJoin(actorWithInnerRoleQuery, "joinedActors")
                .on("actorsIds", SET, "id")
                .toList();

        assertThat(leftWithInnerNestedItems.size(), is(2));
        assertThat(leftWithInnerNestedItems.get(0).joinedActors.size(), is(1));
        assertThat(leftWithInnerNestedItems.get(0).joinedActors.get(0).joinedRoles.size(), is(1));
        assertThat(leftWithInnerNestedItems.get(1).joinedActors.size(), is(0));

        Query<Actor> actorWithLeftRoleQuery = db.query("actors", Actor.class)
                .leftJoin(db.query("roles", Role.class), "joinedRoles")
                .on("name", EQ, "name");

        List<ItemWithJoin> innerWithLeftNestedItems = db.query("items_with_join", ItemWithJoin.class)
                .sort("id", false)
                .innerJoin(actorWithLeftRoleQuery, "joinedActors")
                .on("actorsIds", SET, "id")
                .toList();

        assertThat(innerWithLeftNestedItems.size(), is(2));
        assertThat(innerWithLeftNestedItems.get(0).joinedActors.size(), is(1));
        assertThat(innerWithLeftNestedItems.get(0).joinedActors.get(0).joinedRoles.size(), is(1));
        assertThat(innerWithLeftNestedItems.get(1).joinedActors.size(), is(1));
        assertThat(innerWithLeftNestedItems.get(1).joinedActors.get(0).joinedRoles.size(), is(0));
    }

    @Test
    public void testMergeWithNestedJoins() {
        assumeQueryFormatV2();

        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);
        db.openNamespace("roles", NamespaceOptions.defaultOptions(), Role.class);

        ItemWithJoin firstItem = new ItemWithJoin();
        firstItem.id = 1;
        firstItem.actorsIds = Collections.singletonList(10);
        db.upsert("items_with_join", firstItem);

        ItemWithJoin secondItem = new ItemWithJoin();
        secondItem.id = 2;
        secondItem.actorsIds = Collections.singletonList(20);
        db.upsert("items_with_join", secondItem);

        Actor firstActor = new Actor();
        firstActor.id = 10;
        firstActor.name = "first";
        db.upsert("actors", firstActor);

        Actor secondActor = new Actor();
        secondActor.id = 20;
        secondActor.name = "second";
        db.upsert("actors", secondActor);

        Role firstRole = new Role();
        firstRole.id = 100;
        firstRole.name = "first";
        db.upsert("roles", firstRole);

        Role secondRole = new Role();
        secondRole.id = 200;
        secondRole.name = "second";
        db.upsert("roles", secondRole);

        Query<Actor> firstActorQuery = db.query("actors", Actor.class)
                .innerJoin(db.query("roles", Role.class), "joinedRoles")
                .on("name", EQ, "name");
        Query<Actor> secondActorQuery = db.query("actors", Actor.class)
                .innerJoin(db.query("roles", Role.class), "joinedRoles")
                .on("name", EQ, "name");

        List<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .where("id", EQ, 1)
                .innerJoin(firstActorQuery, "joinedActors")
                .on("actorsIds", SET, "id")
                .merge(db.query("items_with_join", ItemWithJoin.class)
                        .where("id", EQ, 2)
                        .innerJoin(secondActorQuery, "joinedActors")
                        .on("actorsIds", SET, "id"))
                .toList();

        Map<Integer, ItemWithJoin> itemsById = new HashMap<>();
        for (ItemWithJoin item : items) {
            itemsById.put(item.id, item);
        }

        assertThat(itemsById.size(), is(2));
        assertThat(itemsById.get(1).joinedActors.get(0).joinedRoles.get(0).name, is("first"));
        assertThat(itemsById.get(2).joinedActors.get(0).joinedRoles.get(0).name, is("second"));
    }

    @Test
    public void testNestedJoinWithDepthTwo() {
        assumeQueryFormatV2();

        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);
        db.openNamespace("roles", NamespaceOptions.defaultOptions(), Role.class);
        db.openNamespace("permissions", NamespaceOptions.defaultOptions(), Permission.class);

        ItemWithJoin item = new ItemWithJoin();
        item.id = 1;
        item.actorsIds = Collections.singletonList(10);
        db.upsert("items_with_join", item);

        Actor actor = new Actor();
        actor.id = 10;
        actor.name = "actor";
        db.upsert("actors", actor);

        Role role = new Role();
        role.id = 100;
        role.name = "actor";
        db.upsert("roles", role);

        Permission permission = new Permission();
        permission.id = 1000;
        permission.name = "actor";
        db.upsert("permissions", permission);

        Query<Role> roleQuery = db.query("roles", Role.class)
                .innerJoin(db.query("permissions", Permission.class), "joinedPermissions")
                .on("name", EQ, "name");
        Query<Actor> actorQuery = db.query("actors", Actor.class)
                .innerJoin(roleQuery, "joinedRoles")
                .on("name", EQ, "name");

        ItemWithJoin result = db.query("items_with_join", ItemWithJoin.class)
                .where("id", EQ, 1)
                .innerJoin(actorQuery, "joinedActors")
                .on("actorsIds", SET, "id")
                .getOne();

        Actor joinedActor = result.joinedActors.get(0);
        Role joinedRole = joinedActor.joinedRoles.get(0);
        assertThat(joinedRole.id, is(100));
        assertThat(joinedRole.joinedPermissions.size(), is(1));
        assertThat(joinedRole.joinedPermissions.get(0).id, is(1000));
    }

    @Test
    public void testCannotQuerySelectJoinWithMerge() {
        assumeQueryFormatV2();

        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin item = new ItemWithJoin();
        item.id = 1;
        item.actorsIds = Collections.singletonList(10);
        db.upsert("items_with_join", item);

        Actor actor = new Actor();
        actor.id = 10;
        actor.name = "actor";
        db.upsert("actors", actor);

        Query<Actor> actorQuery = db.query("actors", Actor.class)
                .merge(db.query("actors", Actor.class));

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> readAll(db.query("items_with_join", ItemWithJoin.class)
                        .innerJoin(actorQuery, "joinedActors")
                        .on("actorsIds", SET, "id")
                        .execute()));

        assertThat(exception.getMessage(), containsString("MERGEs nested into the JOINs are not supported"));
    }

    @Test
    public void testCannotQuerySelectSubqueryWithJoin() {
        assumeQueryFormatV2();

        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);
        db.openNamespace("roles", NamespaceOptions.defaultOptions(), Role.class);

        ItemWithJoin item = new ItemWithJoin();
        item.id = 1;
        db.upsert("items_with_join", item);

        Actor actor = new Actor();
        actor.id = 1;
        actor.name = "actor";
        db.upsert("actors", actor);

        Query<Actor> subQuery = db.query("actors", Actor.class)
                .select("id")
                .innerJoin(db.query("roles", Role.class), "joinedRoles")
                .on("name", EQ, "name");

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> readAll(db.query("items_with_join", ItemWithJoin.class)
                        .where("id", SET, subQuery)
                        .execute()));

        assertThat(exception.getMessage(), containsString("Join cannot be in subquery"));
    }

    private void assumeQueryFormatV2() {
        assumeTrue(db.getBinding().queryFormatVersion() == Consts.QUERY_FORMAT_V2
                        && db.getBinding().supportsNestedJoinQueries(),
                "Nested joins require QueryFormatV2 and server-side nested join support");
    }

    private void readAll(ResultIterator<?> iterator) {
        try (ResultIterator<?> closeableIterator = iterator) {
            while (closeableIterator.hasNext()) {
                closeableIterator.next();
            }
        }
    }

    @Test
    public void testJoinInWherePartWithBrackets() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin item1 = new ItemWithJoin();
        item1.id = 1;
        item1.name = "resultById";

        ItemWithJoin item2 = new ItemWithJoin();
        item2.id = 101;
        item2.name = "resultByInnerJoinName";
        item2.actorName = "Test";
        item2.actorsIds = Collections.singletonList(103);

        ItemWithJoin item3 = new ItemWithJoin();
        item3.id = 102;
        item3.name = "resultByInnerJoinId";
        item3.actorName = "NotTest";
        item3.actorsIds = Collections.singletonList(201);

        Actor actorByName = new Actor();
        actorByName.id = 201;
        actorByName.name = "Test";

        Actor actorById = new Actor();
        actorById.id = 103;
        actorById.name = "NotTest";

        db.upsert("items_with_join", item1);
        db.upsert("items_with_join", item2);
        db.upsert("items_with_join", item3);
        db.upsert("actors", actorById);
        db.upsert("actors", actorByName);

        ResultIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
                .where("id", RANGE, 0, 100)
                .or()
                .openBracket()
                .innerJoin(db.query("actors", Actor.class)
                        .where("name", EQ, "Test")
                        .on("actorsIds", SET, "id"), "joinedActors")
                .innerJoin(db.query("actors", Actor.class)
                        .where("id", RANGE, 200, 300)
                        .on("actorsIds", SET, "id"), "joinedActors")
                .closeBracket()
                .execute();

        ItemWithJoin resultById = items.next();
        ItemWithJoin resultByInnerJoinNameAndId = items.next();

        assertThat(resultById.id, is(item1.id));
        assertThat(resultByInnerJoinNameAndId.id, is(item3.id));
    }

    @Test
    public void testExecSqlWithJoinForOne() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        ItemWithJoin itemWithJoin = new ItemWithJoin();
        itemWithJoin.id = 1;
        itemWithJoin.name = "name";
        itemWithJoin.actorName = "Test";
        itemWithJoin.actorsIds = new ArrayList<>();
        itemWithJoin.actorsIds.add(1);

        Actor actor = new Actor();
        actor.id = 1;
        actor.name = "Test";
        actor.visible = true;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actor);

        ResultIterator<ItemWithJoin> items = db.execSql(
                "SELECT * FROM items_with_join INNER JOIN actors ON items_with_join.actorName = actors.name",
                ItemWithJoin.class
        );

        assertThat(items.hasNext(), is(true));

        ItemWithJoin result = items.next();
        assertThat(result.id, is(itemWithJoin.id));
        assertThat(result.name, is(itemWithJoin.name));
        assertThat(result.actorName, is(itemWithJoin.actorName));

        assertThat(items.hasNext(), is(false));
    }

    @Test
    public void testExecSqlWithJoinForList() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        for (int i = 0; i < 100; i++) {
            ItemWithJoin itemWithJoin = new ItemWithJoin();
            itemWithJoin.id = i;
            itemWithJoin.name = "name" + i;
            itemWithJoin.actorName = "Test" + i;
            itemWithJoin.actorsIds = new ArrayList<>();
            itemWithJoin.actorsIds.add(1);

            Actor actor = new Actor();
            actor.id = i;
            actor.name = "Test" + i;
            actor.visible = true;

            db.upsert("items_with_join", itemWithJoin);
            db.upsert("actors", actor);
        }

        ResultIterator<ItemWithJoin> items = db.execSql(
                "SELECT * FROM items_with_join INNER JOIN actors ON items_with_join.actorName = actors.name",
                ItemWithJoin.class
        );

        assertThat(items.hasNext(), is(true));

        int count = 0;
        for (int i = 0; i < 100; i++) {
            ItemWithJoin next = items.next();
            assertThat(next.getId(), is(i));
            assertThat(next.getName(), is("name" + i));
            assertThat(next.getActorName(), is("Test" + i));
            count++;
        }
        assertThat(count, is(100));
        assertThat(items.hasNext(), is(false));
    }

    @Test
    public void testQueryWithJoinAndExecuteJson() {
        db.openNamespace("items_with_join", NamespaceOptions.defaultOptions(), ItemWithJoin.class);
        db.openNamespace("actors", NamespaceOptions.defaultOptions(), Actor.class);

        for (int i = 0; i < 3; i++) {
            Actor actor = new Actor();
            actor.id = i;
            actor.name = "ActorName" + i;
            actor.visible = true;
            db.upsert("actors", actor);
        }

        for (int i = 0; i < 6; i++) {
            ItemWithJoin itemWithJoin = new ItemWithJoin();
            itemWithJoin.id = i;
            itemWithJoin.name = "ItemName" + i;
            int actorId = i % 3;
            itemWithJoin.actorName = "ActorName" + actorId;
            itemWithJoin.actorsIds = new ArrayList<>();
            itemWithJoin.actorsIds.add(actorId);
            db.upsert("items_with_join", itemWithJoin);
        }

        QueryResultJsonIterator iterator = db.query("items_with_join", ItemWithJoin.class)
                .where("id", EQ, 1)
                .join(db.query("actors", Actor.class)
                        .on("actorsIds", SET, "id"), "joinedActors")
                .executeToJson();

        assertThat(iterator.hasNext(), is(true));

        String jsonItem = iterator.next();

        Gson gson = new Gson();
        ItemWithJoin item = gson.fromJson(jsonItem, ItemWithJoin.class);
        Actor resultActor = item.getJoinedActors().get(0);

        assertThat(item.id, is(1));
        assertThat(item.name, is("ItemName1"));
        assertThat(resultActor.getId(), is(1));
        assertThat(resultActor.getName(), is("ActorName1"));
        assertThat(resultActor.isVisible(), is(true));
        iterator.close();

        QueryResultJsonIterator fetchAllIterator = db.query("items_with_join", ItemWithJoin.class)
                .join(db.query("actors", Actor.class)
                        .on("actorsIds", SET, "id"), "joinedActors")
                .executeToJson();
        JsonObject jsonWithItems = new JsonParser().parse(fetchAllIterator.fetchAll("items")).getAsJsonObject();

        ItemWithJoin[] items = gson.fromJson(jsonWithItems.getAsJsonArray("items"), ItemWithJoin[].class);

        assertThat(items[5].id, is(5));
        assertThat(items[5].name, is("ItemName5"));
        assertThat(items[5].actorName, is("ActorName2"));
        assertThat(items[5].joinedActors.get(0).name, is("ActorName2"));
    }

    @Setter
    @Getter
    public static class Actor {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "name")
        private String name;

        @Reindex(name = "is_visible")
        private boolean visible;

        @Transient
        private List<Role> joinedRoles;
    }

    @Setter
    @Getter
    public static class Role {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "name")
        private String name;

        @Transient
        private List<Permission> joinedPermissions;
    }

    @Setter
    @Getter
    public static class Permission {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "name")
        private String name;
    }

    @Setter
    @Getter
    public static class ItemWithJoin {

        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "name")
        private String name;

        private List<Integer> actorsIds;

        @Reindex(name = "actorName")
        private String actorName;

        @Transient
        @SerializedName("joined_actors")
        private List<Actor> joinedActors;

        @Transient
        private Actor joinedActor;
    }

}
