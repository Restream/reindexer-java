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
package ru.rt.restream.reindexer.exceptions;

import ru.rt.restream.reindexer.ReindexerResponse;
import ru.rt.restream.reindexer.binding.Consts;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Provides a method for constructing various reindexer exceptions.
 */
public class ReindexerExceptionFactory {

    private static final Map<Integer, Function<ReindexerResponse, ReindexerException>> FACTORIES = new HashMap<>();

    static {
        FACTORIES.put(Consts.ERR_CONFLICT, r -> new IndexConflictException(r.getErrorMessage()));
        FACTORIES.put(Consts.ERR_STATE_INVALIDATED, r -> new StateInvalidatedException(r.getErrorMessage()));
    }

    /**
     * Creates new exception from the reindexer server response.
     *
     * @param response the reindexer response
     */
    public static ReindexerException fromResponse(ReindexerResponse response) {
        if (response.getCode() == 0) {
            throw new IllegalArgumentException("Not an error response");
        }
        return FACTORIES.getOrDefault(response.getCode(), code -> new ReindexerException(response.getErrorMessage()))
                .apply(response);
    }

}
