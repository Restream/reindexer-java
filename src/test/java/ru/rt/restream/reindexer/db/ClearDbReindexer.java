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

import ru.rt.restream.reindexer.Reindexer;
import ru.rt.restream.reindexer.ReindexerNamespace;
import ru.rt.restream.reindexer.binding.Binding;

/**
 * Only for test purposes.
 * Allows to remove all registered namespaces.
 */
public class ClearDbReindexer extends Reindexer {

    ClearDbReindexer(Binding binding) {
        super(binding);
    }

    /**
     * Removes all registered namespaces.
     * TODO: to do refactoring after implementation of Reindexer.enumNamespaces
     */
    void clear() {
        Binding binding = getBinding();
        namespaceMap.values().stream()
                .map(ReindexerNamespace::getName)
                // skip service namespaces
                .filter(name -> !name.startsWith("#"))
                .distinct()
                .forEach(binding::dropNamespace);
        namespaceMap.clear();
    }

}
