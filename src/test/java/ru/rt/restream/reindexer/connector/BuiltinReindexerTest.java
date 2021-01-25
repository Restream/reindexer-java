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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;
import ru.rt.restream.reindexer.Configuration;

import java.io.File;
import java.io.IOException;

/**
 * Tests for Builtin implementation.
 */
public class BuiltinReindexerTest extends ReindexerTest {

    private static final String TEST_DB_PATH = "/tmp/reindex/test_items";

    @BeforeEach
    void setUp() {
        db = Configuration.builder()
                .url("builtin://" + TEST_DB_PATH)
                .getReindexer();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (db != null) {
            db.close();
            FileUtils.deleteDirectory(new File(TEST_DB_PATH));
        }
    }

}
