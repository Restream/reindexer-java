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

import ru.rt.restream.reindexer.db.DbLocator;

import static ru.rt.restream.reindexer.db.DbLocator.Type.BUILTIN;

/**
 * Tests for Builtin implementation.
 */
public class BuiltinMergeTest extends MergeTest {

    @Override
    protected DbLocator.Type getDbType() {
        return BUILTIN;
    }

}
