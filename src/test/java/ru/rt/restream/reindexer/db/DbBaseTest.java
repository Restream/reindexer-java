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

package ru.rt.restream.reindexer.db;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * A base class for all of the test classes that use Reindexer instance.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DbCloseExtension.class)
public abstract class DbBaseTest {

    protected ClearDbReindexer db;

    /**
     * Return BUILTIN or CPROTO.
     */
    protected abstract DbLocator.Type getDbType();

    @BeforeAll
    protected void initDb(){
        db = DbLocator.getDb(getDbType());
    }

    /**
     * Clear Reindexer instance after each test method.
     */
    @AfterEach
    void clearDb() {
        db.clear();
    }
}
