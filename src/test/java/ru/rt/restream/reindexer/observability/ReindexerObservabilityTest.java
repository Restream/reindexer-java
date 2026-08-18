/*
 * Copyright 2020-present Restream
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
package ru.rt.restream.reindexer.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.exporter.FinishedSpan;
import io.micrometer.tracing.test.SampleTestRunner;
import lombok.Data;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.rt.restream.reindexer.Namespace;
import ru.rt.restream.reindexer.NamespaceOptions;
import ru.rt.restream.reindexer.Query;
import ru.rt.restream.reindexer.Transaction;
import ru.rt.restream.reindexer.annotations.Reindex;
import ru.rt.restream.reindexer.db.ClearDbReindexer;
import ru.rt.restream.reindexer.db.DbCloseExtension;
import ru.rt.restream.reindexer.db.DbLocator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Reindexer observability.
 */
@ExtendWith(DbCloseExtension.class)
public class ReindexerObservabilityTest extends SampleTestRunner {

    private static final SimpleMeterRegistry METER_REGISTRY = new SimpleMeterRegistry();

    private static final ObservationRegistry OBSERVATION_REGISTRY = ObservationRegistry.create();

    static {
        OBSERVATION_REGISTRY.observationConfig().observationHandler(new DefaultMeterObservationHandler(METER_REGISTRY));
    }

    private static ClearDbReindexer db;

    @BeforeAll
    static void beforeAll() {
        db = DbLocator.getDb(DbLocator.Type.OBSERVATION, OBSERVATION_REGISTRY);
    }

    @Override
    protected MeterRegistry createMeterRegistry() {
        return METER_REGISTRY;
    }

    @Override
    protected ObservationRegistry createObservationRegistry() {
        return OBSERVATION_REGISTRY;
    }

    @AfterEach
    void tearDown() {
        if (db != null) {
            db.clear();
        }
    }

    @Override
    public SampleTestRunnerConsumer yourCode() {
        return (tracer, meterRegistry) -> {
            String namespaceName = "items";
            Namespace<TestItem> namespace = db.openNamespace(namespaceName, NamespaceOptions.defaultOptions(), TestItem.class);

            Transaction<TestItem> tx = namespace.beginTransaction();
            TestItem testItem = new TestItem();
            testItem.setId(123);
            testItem.setName("TestName");
            testItem.setValue("TestValue");
            tx.insert(testItem);

            tx.query()
                    .where("id", Query.Condition.EQ, 123)
                    .set("name", "UpdatedName")
                    .update();

            tx.query()
                    .where("id", Query.Condition.EQ, 123)
                    .delete();

            tx.commit();

            boolean exists = namespace.query()
                    .where("id", Query.Condition.EQ, 123)
                    .exists();
            assertThat(exists).isFalse();

            namespace.query()
                    .where("id", Query.Condition.EQ, 123)
                    .set("name", "UpdatedName")
                    .update();

            namespace.query()
                    .where("id", Query.Condition.EQ, 123)
                    .delete();

            System.out.println(METER_REGISTRY.getMetersAsString());

            assertThat(tracer.getFinishedSpans())
                    .hasSize(14)
                    .extracting(FinishedSpan::getName)
                    .contains(
                            "reindexer.rpc.openNamespace",
                            "reindexer.rpc.addIndex",
                            "reindexer.rpc.addIndex",
                            "reindexer.rpc.addIndex",
                            "reindexer.rpc.startTransaction",
                            "reindexer.rpc.addTxItem",
                            "reindexer.rpc.selectQuery",
                            "reindexer.rpc.addTxItem",
                            "reindexer.rpc.commitTx",
                            "reindexer.rpc.selectQuery",
                            "reindexer.rpc.updateQuery",
                            "reindexer.rpc.updateQueryTx",
                            "reindexer.rpc.deleteQuery",
                            "reindexer.rpc.deleteQueryTx"
                    );
        };
    }

    @Data
    public static class TestItem {
        @Reindex(name = "id", isPrimaryKey = true)
        private Integer id;
        @Reindex(name = "name")
        private String name;
        @Reindex(name = "value")
        private String value;
    }

}
