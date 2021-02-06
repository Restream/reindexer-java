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

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import ru.rt.restream.reindexer.ReindexerConfiguration;
import ru.rt.restream.reindexer.Reindexer;

import java.io.File;
import java.io.IOException;
import java.time.Duration;

public class CprotoMergeTest extends MergeTest {

    private Reindexer server;

    @BeforeEach
    public void setUp() {
        server = ReindexerConfiguration.builder()
                .url("builtinserver://items")
                .getReindexer();
        db = ReindexerConfiguration.builder()
                .url("cproto://localhost:6534/items")
                .connectionPoolSize(4)
                .requestTimeout(Duration.ofSeconds(30L))
                .getReindexer();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (server != null) {
            server.close();
            FileUtils.deleteDirectory(new File("/tmp/reindex/items"));
        }
        if (db != null) {
            db.close();
        }
    }

}
