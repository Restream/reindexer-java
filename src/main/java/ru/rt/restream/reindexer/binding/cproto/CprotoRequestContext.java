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
package ru.rt.restream.reindexer.binding.cproto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.binding.QueryResult;
import ru.rt.restream.reindexer.binding.RequestContext;
import ru.rt.restream.reindexer.binding.cproto.cjson.PayloadField;
import ru.rt.restream.reindexer.binding.cproto.cjson.PayloadType;
import ru.rt.restream.reindexer.binding.cproto.util.ConnectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * A request context which establish a connection to the Reindexer instance via RPC.
 */
public class CprotoRequestContext implements RequestContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(CprotoRequestContext.class);

    private static final int QUERY_RESULT_END = 0;

    private static final int FETCH_RESULTS = 50;

    private static final int CLOSE_RESULTS = 51;

    private static final int RESULTS_FORMAT_MASK = 0xF;

    private static final int RESULTS_JSON = 0x3;

    private static final int RESULTS_WITH_RANK = 0x40;

    private static final int RESULTS_WITH_ITEM_ID = 0x20;

    private static final int RESULTS_WITH_NS_ID = 0x80;

    private static final int RESULTS_WITH_PAYLOAD_TYPES = 0x10;

    private final Connection connection;

    private final boolean transactional;

    private QueryResult queryResult;

    private int requestId = -1;

    /**
     * Creates an instance.
     *
     * @param rpcResponse   the RPC response
     * @param connection    the connection in which the request was made
     * @param transactional indicates that the request is being executed in a transaction
     */
    public CprotoRequestContext(RpcResponse rpcResponse, Connection connection, boolean transactional) {
        this.queryResult = getQueryResult(rpcResponse);
        this.connection = connection;
        this.transactional = transactional;
    }

    @Override
    public QueryResult getQueryResult() {
        return queryResult;
    }

    @Override
    public void fetchResults(int offset, int limit) {
        int flags = 0;
        if (queryResult.isJson()) {
            flags = Consts.RESULTS_JSON;
        } else {
            flags |= Consts.RESULTS_C_JSON | Consts.RESULTS_WITH_PAYLOAD_TYPES | Consts.RESULTS_WITH_ITEM_ID;
        }
        int fetchCount = limit <= 0 ? Integer.MAX_VALUE : limit;
        RpcResponse rpcResponse = ConnectionUtils.rpcCall(connection, FETCH_RESULTS, requestId, flags, offset, fetchCount);
        queryResult = getQueryResult(rpcResponse);
    }

    /**
     * Closes query results if need (i.e. query request id is not -1).
     * Closes the connection if the request is not transactional (the connection will be closed after a rollback or commit).
     */
    @Override
    public void close() {
        try {
            if (requestId != -1) {
                RpcResponse rpcResponse = connection.rpcCall(CLOSE_RESULTS, requestId);
                if (rpcResponse.hasError()) {
                    LOGGER.error("rx: query close error {}", rpcResponse.getErrorMessage());
                }
                requestId = -1;
            }
        } finally {
            if (!transactional) {
                ConnectionUtils.close(connection);
            }
        }
    }

    private QueryResult getQueryResult(RpcResponse rpcResponse) {
        byte[] rawQueryResult = new byte[0];
        Object[] responseArguments = rpcResponse.getArguments();
        if (responseArguments.length > 0) {
            rawQueryResult = (byte[]) responseArguments[0];
        }
        if (responseArguments.length > 1) {
            requestId = (int) responseArguments[1];
        }

        ByteBuffer buffer = new ByteBuffer(rawQueryResult).rewind();
        long flags = buffer.getVarUInt();
        boolean isJson = (flags & RESULTS_FORMAT_MASK) == RESULTS_JSON;
        boolean withRank = (flags & RESULTS_WITH_RANK) != 0;
        boolean withItemId = (flags & RESULTS_WITH_ITEM_ID) != 0;
        boolean withNsId = (flags & RESULTS_WITH_NS_ID) != 0;
        boolean withPayloadTypes = (flags & RESULTS_WITH_PAYLOAD_TYPES) != 0;

        QueryResult queryResult = new QueryResult();
        queryResult.setJson(isJson);
        queryResult.setWithRank(withRank);
        queryResult.setTotalCount(buffer.getVarUInt());
        queryResult.setqCount(buffer.getVarUInt());
        queryResult.setCount(buffer.getVarUInt());
        queryResult.setWithItemId(withItemId);
        queryResult.setWithNsId(withNsId);
        queryResult.setWithPayloadTypes(withPayloadTypes);

        List<PayloadType> payloadTypes = new ArrayList<>();
        queryResult.setPayloadTypes(payloadTypes);
        if (!isJson && queryResult.isWithPayloadTypes()) {
            int ptCount = (int) buffer.getVarUInt();
            for (int i = 0; i < ptCount; i++) {
                long namespaceId = buffer.getVarUInt();
                String namespaceName = buffer.getVString();
                long stateToken = buffer.getVarUInt();
                long version = buffer.getVarUInt();

                //read tags
                List<String> tags = new ArrayList<>();
                long tagsCount = buffer.getVarUInt();
                for (int j = 0; j < tagsCount; j++) {
                    tags.add(buffer.getVString());
                }

                //read payload fields
                long pStringHdrOffset = buffer.getVarUInt();
                List<PayloadField> fields = new ArrayList<>();
                long fieldsCount = buffer.getVarUInt();
                for (int j = 0; j < fieldsCount; j++) {
                    long type = buffer.getVarUInt();
                    String name = buffer.getVString();
                    long offset = buffer.getVarUInt();
                    long size = buffer.getVarUInt();
                    boolean isArray = buffer.getVarUInt() != 0;
                    long jsonPathCnt = buffer.getVarUInt();
                    List<String> jsonPaths = new ArrayList<>();
                    for (int k = 0; k < jsonPathCnt; k++) {
                        jsonPaths.add(buffer.getVString());
                    }
                    fields.add(new PayloadField(type, name, offset, size, isArray, jsonPaths));
                }

                PayloadType payloadType = new PayloadType(namespaceId, namespaceName, version, stateToken,
                        pStringHdrOffset, tags, fields);
                payloadTypes.add(payloadType);
            }
        }

        readExtraResults(buffer);
        queryResult.setBuffer(new ByteBuffer(buffer.getBytes()).rewind());
        return queryResult;
    }

    private void readExtraResults(ByteBuffer buffer) {
        int tag = (int) buffer.getVarUInt();
        while (tag != QUERY_RESULT_END) {
            byte[] data = buffer.getBytes((int) buffer.getUInt32());
            tag = (int) buffer.getVarUInt();
        }
    }

}
