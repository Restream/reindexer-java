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

import lombok.Getter;
import lombok.Setter;
import ru.rt.restream.reindexer.AggregationResult;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.binding.cproto.cjson.PayloadType;

import java.util.List;

/**
 * Result of a query.
 */
@Getter
@Setter
public class QueryResult {

    /**
     * Result total count.
     */
    private long totalCount;

    /**
     * The current query item count.
     */
    private long qCount;

    /**
     * The current query fetched items count.
     */
    private long count;

    /**
     * The query result pointer (for builtin bindings)
     */
    private long resultsPtr;

    /**
     * An indication that the query result is in json format.
     */
    private boolean isJson;

    /**
     * An indication that the query result contains rank values.
     */
    private boolean withRank;

    /**
     * An indication that the query result contains shard ID values.
     */
    private boolean withShardId;

    /**
     * An indication that the query result contains item ids.
     */
    private boolean withItemId;

    /**
     * An indication that the query result contains namespace ids.
     */
    private boolean withNsId;

    /**
     * An indication that the query result contains payload types.
     */
    private boolean withPayloadTypes;

    /**
     * An indication that the query result contains result pointer (for builtin bindings).
     */
    private boolean withResultsPtr;

    /**
     * The current query result payload types.
     */
    private List<PayloadType> payloadTypes;

    /**
     * An indication that the query result contains joined data.
     */
    private boolean withJoined;

    /**
     * The current query result aggregation results.
     */
    private List<AggregationResult> aggResults;

    /**
     * Get the {@link ByteBuffer} with query result data
     */
    private ByteBuffer buffer;

    /**
     * The global shard ID value for query result.
     */
    private int shardId = Consts.SHARDING_PROXY_OFF;

    /**
     * Version of the sharding config.
     */
    private long shardingVersion;

    /**
     * Numeric value of rank format.
     *
     * <p>It supports only {@link ru.rt.restream.reindexer.binding.Consts#RANK_FORMAT_SINGLE_FLOAT}.
     */
    private long rankFormat;

}
