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

package ru.rt.restream.reindexer.binding;

import com.google.gson.Gson;
import ru.rt.restream.reindexer.AggregationResult;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.binding.cproto.cjson.PayloadField;
import ru.rt.restream.reindexer.binding.cproto.cjson.PayloadType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ru.rt.restream.reindexer.binding.Consts.QUERY_RESULT_AGGREGATION;
import static ru.rt.restream.reindexer.binding.Consts.QUERY_RESULT_END;
import static ru.rt.restream.reindexer.binding.Consts.QUERY_RESULT_EXPLAIN;
import static ru.rt.restream.reindexer.binding.Consts.QUERY_RESULT_INCARNATION_TAGS;
import static ru.rt.restream.reindexer.binding.Consts.QUERY_RESULT_RANK_FORMAT;
import static ru.rt.restream.reindexer.binding.Consts.QUERY_RESULT_SHARDING_VERSION;
import static ru.rt.restream.reindexer.binding.Consts.QUERY_RESULT_SHARD_ID;
import static ru.rt.restream.reindexer.binding.Consts.RANK_FORMAT_SINGLE_FLOAT;
import static ru.rt.restream.reindexer.binding.Consts.RESULTS_FORMAT_MASK;
import static ru.rt.restream.reindexer.binding.Consts.RESULTS_JSON;
import static ru.rt.restream.reindexer.binding.Consts.RESULTS_PTRS;
import static ru.rt.restream.reindexer.binding.Consts.RESULTS_WITH_ITEM_ID;
import static ru.rt.restream.reindexer.binding.Consts.RESULTS_WITH_JOINED;
import static ru.rt.restream.reindexer.binding.Consts.RESULTS_WITH_NS_ID;
import static ru.rt.restream.reindexer.binding.Consts.RESULTS_WITH_PAYLOAD_TYPES;
import static ru.rt.restream.reindexer.binding.Consts.RESULTS_WITH_RANK;
import static ru.rt.restream.reindexer.binding.Consts.RESULTS_WITH_SHARD_ID;
import static ru.rt.restream.reindexer.binding.Consts.VALUE_FLOAT_VECTOR;

/**
 * A {@link QueryResult} reader.
 */
public class QueryResultReader {

    private final Gson gson = new Gson();

    /**
     * Reads a {@link QueryResult} from the raw byte array.
     *
     * @param rawQueryResult the raw byte array
     * @return the {@link QueryResult} to use
     */
    public QueryResult read(byte[] rawQueryResult) {
        ByteBuffer buffer = new ByteBuffer(rawQueryResult).rewind();
        QueryResult queryResult = getQueryResultWithFlags(buffer.getVarUInt());
        queryResult.setTotalCount(buffer.getVarUInt());
        queryResult.setQCount(buffer.getVarUInt());
        queryResult.setCount(buffer.getVarUInt());

        List<PayloadType> payloadTypes = new ArrayList<>();
        queryResult.setPayloadTypes(payloadTypes);
        if (!queryResult.isJson() && queryResult.isWithPayloadTypes()) {
            int ptCount = (int) buffer.getVarUInt();
            for (int i = 0; i < ptCount; i++) {
                long namespaceId = buffer.getVarUInt();
                String namespaceName = buffer.getVString();
                int stateToken = (int) buffer.getVarUInt();
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

                // see reindexer/cjson/creflect.go::Read
                long fieldsCount = buffer.getVarUInt();
                for (int j = 0; j < fieldsCount; j++) {
                    long type = buffer.getVarUInt();
                    int floatVectorDimension = (type == VALUE_FLOAT_VECTOR)
                            ? (int) buffer.getVarUInt()
                            : 0;
                    String name = buffer.getVString();
                    long offset = buffer.getVarUInt();
                    long size = buffer.getVarUInt();
                    boolean isArray = buffer.getVarUInt() != 0;
                    long jsonPathCnt = buffer.getVarUInt();
                    List<String> jsonPaths = new ArrayList<>();
                    for (int k = 0; k < jsonPathCnt; k++) {
                        jsonPaths.add(buffer.getVString());
                    }
                    fields.add(new PayloadField(type, name, offset, size, isArray, jsonPaths, floatVectorDimension));
                }
                PayloadType payloadType = new PayloadType(namespaceId, namespaceName, version, stateToken,
                        pStringHdrOffset, tags, fields);
                payloadTypes.add(payloadType);
            }
        }

        // result_serializer.go::readExtraResults
        int tag = (int) buffer.getVarUInt();
        ArrayList<AggregationResult> aggregationResults = new ArrayList<>();
        while (tag != QUERY_RESULT_END) {
            switch (tag) {
                case QUERY_RESULT_AGGREGATION:
                    byte[] data = buffer.getBytes((int) buffer.getUInt32());
                    aggregationResults.add(deserializeAggResult(data));
                    break;
                case QUERY_RESULT_EXPLAIN:
                    buffer.getBytes((int) buffer.getUInt32());
                    break;
                case QUERY_RESULT_SHARDING_VERSION:
                    queryResult.setShardingVersion(buffer.getVarInt());
                    break;
                case QUERY_RESULT_SHARD_ID:
                    queryResult.setShardId((int) buffer.getVarUInt());
                    break;
                case QUERY_RESULT_INCARNATION_TAGS:
                    int shardsCnt = (int) buffer.getVarUInt();
                    Map<Integer, long[]> incarnationTags = new HashMap<>();
                    queryResult.setIncarnationTags(incarnationTags);
                    for (int i = 0; i < shardsCnt; i++) {
                        int shardId = (int) buffer.getVarInt();
                        int nsCnt = (int) buffer.getVarUInt();
                        if (nsCnt > 0) {
                            long[] sl = new long[nsCnt];
                            for (int j = 0; j < nsCnt; j++) {
                                sl[j] = buffer.getVarInt();
                            }
                            incarnationTags.put(shardId, sl);
                        }
                    }
                    break;
                case QUERY_RESULT_RANK_FORMAT:
                    int format = (int) buffer.getVarUInt();
                    if (format != RANK_FORMAT_SINGLE_FLOAT) {
                        String errorMessage = String.format("Unexpected rank format value: %d - only supported format is 0 (single float rank)", format);
                        throw new RuntimeException(errorMessage);
                    }
                    queryResult.setRankFormat(format);
                    break;
                default:
                    throw new RuntimeException("Illegal QueryResults tag: " + tag);
            }
            tag = (int) buffer.getVarUInt();
        }
        queryResult.setAggResults(aggregationResults);
        queryResult.setBuffer(new ByteBuffer(buffer.getBytes()).rewind());

        return queryResult;
    }

    private static QueryResult getQueryResultWithFlags(long flags) {
        boolean isJson = (flags & RESULTS_FORMAT_MASK) == RESULTS_JSON;
        boolean withRank = (flags & RESULTS_WITH_RANK) != 0;
        boolean withItemId = (flags & RESULTS_WITH_ITEM_ID) != 0;
        boolean withNsId = (flags & RESULTS_WITH_NS_ID) != 0;
        boolean withPayloadTypes = (flags & RESULTS_WITH_PAYLOAD_TYPES) != 0;
        boolean withJoined = (flags & RESULTS_WITH_JOINED) != 0;
        boolean withResultsPtr = (flags & RESULTS_PTRS) != 0;
        boolean withShardId = (flags & RESULTS_WITH_SHARD_ID) != 0;
        QueryResult queryResult = new QueryResult();
        queryResult.setJson(isJson);
        queryResult.setWithRank(withRank);
        queryResult.setWithItemId(withItemId);
        queryResult.setWithNsId(withNsId);
        queryResult.setWithPayloadTypes(withPayloadTypes);
        queryResult.setWithJoined(withJoined);
        queryResult.setWithResultsPtr(withResultsPtr);
        queryResult.setWithShardId(withShardId);
        return queryResult;
    }

    private AggregationResult deserializeAggResult(byte[] bytes) {
        String json = new String(bytes, StandardCharsets.UTF_8);
        return gson.fromJson(json, AggregationResult.class);
    }
}
