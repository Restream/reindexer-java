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
package ru.rt.restream.reindexer.binding.cproto;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.rt.restream.reindexer.ReindexerResponse;
import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.binding.QueryResult;
import ru.rt.restream.reindexer.binding.QueryResultReader;
import ru.rt.restream.reindexer.binding.RequestContext;
import ru.rt.restream.reindexer.binding.cproto.util.ConnectionUtils;

/**
 * A request context which establish a connection to the Reindexer instance via RPC.
 */
public class CprotoRequestContext implements RequestContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(CprotoRequestContext.class);

    private static final int FETCH_RESULTS = 50;

    private static final int CLOSE_RESULTS = 51;

    private final QueryResultReader reader = new QueryResultReader();

    private static final int RESULTS_WITH_JOINED = 0x100;

    private final Connection connection;

    private final Gson gson = new Gson();

    private final boolean asJson;

    private QueryResult queryResult;

    private int requestId = -1;

    /**
     * Creates an instance.
     *
     * @param rpcResponse the RPC response
     * @param connection  the connection in which the request was made
     */
    public CprotoRequestContext(ReindexerResponse rpcResponse, Connection connection, boolean asJson) {
        this.queryResult = getQueryResult(rpcResponse);
        this.connection = connection;
        this.asJson = asJson;
    }

    @Override
    public QueryResult getQueryResult() {
        return queryResult;
    }

    @Override
    public void fetchResults(int offset, int limit) {
        int flags = asJson
                ? Consts.RESULTS_JSON
                : Consts.RESULTS_C_JSON | Consts.RESULTS_WITH_PAYLOAD_TYPES | Consts.RESULTS_WITH_ITEM_ID;
        int fetchCount = limit <= 0 ? Integer.MAX_VALUE : limit;
        ReindexerResponse rpcResponse = ConnectionUtils.rpcCall(connection, FETCH_RESULTS, requestId, flags, offset, fetchCount);
        queryResult = getQueryResult(rpcResponse);
    }

    /**
     * Closes query results if need (i.e. query request id is not -1).
     */
    @Override
    public void closeResults() {
        if (requestId != -1) {
            ReindexerResponse rpcResponse = connection.rpcCall(CLOSE_RESULTS, requestId);
            if (rpcResponse.hasError()) {
                LOGGER.error("rx: query close error {}", rpcResponse.getErrorMessage());
            }
            requestId = -1;
        }
    }

    private QueryResult getQueryResult(ReindexerResponse rpcResponse) {
        byte[] rawQueryResult = new byte[0];
        Object[] responseArguments = rpcResponse.getArguments();
        if (responseArguments.length > 0) {
            rawQueryResult = (byte[]) responseArguments[0];
        }
        if (responseArguments.length > 1) {
            requestId = (int) responseArguments[1];
        }
        return reader.read(rawQueryResult);
    }

}
