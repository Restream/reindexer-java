/**
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
package ru.rt.restream.reindexer.exceptions;

import ru.rt.restream.reindexer.binding.Consts;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ReindexerExceptionFactory {

    private static final Map<Integer, Function<Integer, ReindexerException>> FACTORIES = new HashMap<>();

    public ReindexerExceptionFactory() {
        FACTORIES.put(Consts.ERR_CONFLICT, code -> new IndexConflictException());
    }

    public static ReindexerException fromErrorCode(int code) {
        return FACTORIES.getOrDefault(code, c -> new ReindexerException())
                .apply(code);
    }

}
