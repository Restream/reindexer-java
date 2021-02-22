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

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;
import static org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;

/**
 * Closes Reindexer instances after completing all tests.
 */
public class DbCloseExtension implements BeforeAllCallback, CloseableResource {

    private static boolean isTestsStarted;

    @Override
    public void beforeAll(ExtensionContext context) {
        if (!isTestsStarted) {
            isTestsStarted = true;
            // Registration of extension for invoke of this.close() after ending of all tests
            context.getRoot().getStore(GLOBAL).put("DbCloseExtension", this);
        }
    }

    @Override
    public void close() throws Throwable {
        DbLocator.closeAllDbInstances();
    }

}
