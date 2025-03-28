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

package ru.rt.restream.util;

import ru.rt.restream.reindexer.Namespace;
import ru.rt.restream.reindexer.ReindexerIndex;
import ru.rt.restream.reindexer.ReindexerNamespace;

import java.util.List;
import java.util.Objects;

public class ReindexerUtils {
    public static ReindexerIndex getIndexByName(List<ReindexerIndex> indexes, String indexName) {
        for (ReindexerIndex index : indexes) {
            if (Objects.equals(index.getName(), indexName)) {
                return index;
            }
        }
        return null;
    }

    public static ReindexerIndex getIndexByName(Namespace<?> ns, String name) {
        return ((ReindexerNamespace<?>) ns).getIndexes().stream()
                .filter(i -> i.getName().equals(name))
                .findFirst()
                .orElse(null);
    }
}
