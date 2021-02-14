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

import org.junit.jupiter.api.Test;
import ru.rt.restream.reindexer.CloseableIterator;
import ru.rt.restream.reindexer.Query;
import ru.rt.restream.reindexer.annotations.Reindex;
import ru.rt.restream.reindexer.annotations.Transient;
import ru.rt.restream.reindexer.binding.option.NamespaceOptions;
import ru.rt.restream.reindexer.db.DbBaseTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
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

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

        Actor actor = new Actor();
        actor.id = 1;
        actor.name = "Test";
        actor.visible = true;

        db.upsert("items_with_join", itemWithJoin);
        db.upsert("actors", actor);

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

        CloseableIterator<ItemWithJoin> items = db.query("items_with_join", ItemWithJoin.class)
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

    public static class Actor {

        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "name")
        private String name;

        @Reindex(name = "is_visible")
        private boolean visible;

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

        public boolean isVisible() {
            return visible;
        }

        public void setVisible(boolean visible) {
            this.visible = visible;
        }

    }

    public static class ItemWithJoin {

        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;

        @Reindex(name = "name")
        private String name;

        private List<Integer> actorsIds;

        private String actorName;

        @Transient
        private List<Actor> joinedActors;

        @Transient
        private Actor joinedActor;

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

        public String getActorName() {
            return actorName;
        }

        public void setActorName(String actorName) {
            this.actorName = actorName;
        }

        public List<Actor> getJoinedActors() {
            return joinedActors;
        }

        public void setJoinedActors(List<Actor> joinedActors) {
            this.joinedActors = joinedActors;
        }

        public Actor getJoinedActor() {
            return joinedActor;
        }

        public void setJoinedActor(Actor joinedActor) {
            this.joinedActor = joinedActor;
        }
    }

}
