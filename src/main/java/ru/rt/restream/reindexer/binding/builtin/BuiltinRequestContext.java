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

package ru.rt.restream.reindexer.binding.builtin;

import ru.rt.restream.reindexer.ReindexerResponse;
import ru.rt.restream.reindexer.binding.QueryResult;
import ru.rt.restream.reindexer.binding.QueryResultReader;
import ru.rt.restream.reindexer.binding.RequestContext;
import ru.rt.restream.reindexer.util.NativeUtils;

/**
 * A request context which is holds a {@link QueryResult},
 * the {@link #fetchResults(int, int)} method is NOOP since Builtin does not support it.
 */
public class BuiltinRequestContext implements RequestContext {

    private final QueryResult queryResult;

    /**
     * Creates an instance.
     *
     * @param response the {@link ReindexerResponse} to use
     */
    public BuiltinRequestContext(ReindexerResponse response) {
        long resultsPtr = 0L;
        byte[] rawQueryResult = new byte[0];
        Object[] arguments = response.getArguments();
        if (arguments.length > 0) {
            Object arg = arguments[0];
            if (arg instanceof Long) {
                resultsPtr = (long) arg;
            }
        }
        if (arguments.length > 1) {
            Object arg = arguments[1];
            if (arg instanceof byte[]) {
                rawQueryResult = (byte[]) arg;
            }
        }
        QueryResultReader reader = new QueryResultReader();
        queryResult = reader.read(rawQueryResult);
        queryResult.setResultsPtr(resultsPtr);
    }

    @Override
    public QueryResult getQueryResult() {
        return queryResult;
    }

    @Override
    public void fetchResults(int offset, int limit) {
        // NOOP
    }

    @Override
    public void closeResults() {
        if (queryResult.getResultsPtr() != 0L) {
            NativeUtils.freeNativeBuffer(queryResult.getResultsPtr());
            queryResult.setResultsPtr(0L);
        }
    }

}
