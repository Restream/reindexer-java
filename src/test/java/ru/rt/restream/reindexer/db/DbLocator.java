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

import org.apache.commons.io.FileUtils;
import ru.rt.restream.reindexer.Reindexer;
import ru.rt.restream.reindexer.ReindexerConfiguration;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Creates a Reindexer instances for tests.
 * This class is not thread safe.
 */
public class DbLocator {

    private static final String BUILTIN_DB_PATH = "/tmp/reindex/builtin_test";

    // if change the path, need to synchronized it with path in default-builtin-server-config.yml
    private static final String BUILTINSERVER_DB_PATH = "/tmp/reindex/server";

    private static final Map<Type, ClearDbReindexer> instancesForUse = new HashMap<>();

    private static final Map<ClearDbReindexer, String> instancesForClose = new HashMap<>();

    public static ClearDbReindexer getDb(Type type) {
        ClearDbReindexer db = instancesForUse.get(type);
        if (db == null) {
            db = addDbInstance(type);
        }
        return db;
    }

    static void closeAllDbInstances() throws IOException {
        for (Map.Entry<ClearDbReindexer, String> entry : instancesForClose.entrySet()) {
            Reindexer db = entry.getKey();
            String path = entry.getValue();
            if (db != null) {
                db.close();
            }
            if (path != null) {
                FileUtils.deleteDirectory(new File(path));
            }
        }
        instancesForClose.clear();
        instancesForUse.clear();
    }

    private static ClearDbReindexer addDbInstance(Type type) {
        switch (type) {
            case BUILTIN:
                ClearDbReindexer builtinDb = new ClearDbReindexer(ReindexerConfiguration.builder()
                        .url("builtin://" + BUILTIN_DB_PATH)
                        .getReindexer().getBinding());
                instancesForUse.put(Type.BUILTIN, builtinDb);
                instancesForClose.put(builtinDb, BUILTIN_DB_PATH);
                return builtinDb;
            case CPROTO:
                ClearDbReindexer server = new ClearDbReindexer(ReindexerConfiguration.builder()
                        .url("builtinserver://items")
                        .getReindexer().getBinding());
                // builtinserver is not for use, only for cproto db
                instancesForClose.put(server, BUILTINSERVER_DB_PATH);
                ClearDbReindexer cprotoDb = new ClearDbReindexer(ReindexerConfiguration.builder()
                        .url("cproto://localhost:6534/items")
                        .connectionPoolSize(4)
                        .requestTimeout(Duration.ofSeconds(30L))
                        .getReindexer().getBinding());
                instancesForUse.put(Type.CPROTO, cprotoDb);
                instancesForClose.put(cprotoDb, null);
                return cprotoDb;
            default:
                throw new IllegalArgumentException("Type of Reindexer DB " + type + " is not supported");
        }
    }

    public enum Type {
        BUILTIN,
        CPROTO
    }

}
