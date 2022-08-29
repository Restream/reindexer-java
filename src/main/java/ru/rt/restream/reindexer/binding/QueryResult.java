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

import ru.rt.restream.reindexer.AggregationResult;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.binding.cproto.cjson.PayloadType;

import java.util.List;

/**
 * Result of a query.
 */
public class QueryResult {

    private long totalCount;

    private long qCount;

    private long count;

    private long resultsPtr;

    private boolean isJson;

    private boolean withRank;

    private boolean withShardID;

    private boolean withItemId;

    private boolean withNsId;

    private boolean withPayloadTypes;

    private boolean withResultsPtr;

    private List<PayloadType> payloadTypes;

    private boolean withJoined;

    private List<AggregationResult> aggResults;

    private ByteBuffer buffer;

    private int shardID = Consts.SHARDING_PROXY_OFF;

    private long shardingVersion;

    /**
     * Get the current query result total count.
     *
     * @return the current query total count
     */
    public long getTotalCount() {
        return totalCount;
    }

    /**
     * Set the current total count.
     *
     * @param totalCount total count
     */
    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    /**
     * Get the current query count.
     *
     * @return the current query item count
     */
    public long getQCount() {
        return qCount;
    }

    /**
     * Set the current query count.
     *
     * @param qCount query count
     */
    public void setqCount(long qCount) {
        this.qCount = qCount;
    }

    /**
     * Get the current query result fetched items count.
     *
     * @return the current query fetched items count
     */
    public long getCount() {
        return count;
    }

    /**
     * Set the current query result fetched items count.
     *
     * @param count number of fetched items
     */
    public void setCount(long count) {
        this.count = count;
    }

    /**
     * Get the query result pointer (for builtin bindings).
     *
     * @return the query result pointer
     */
    public long getResultsPtr() {
        return resultsPtr;
    }

    /**
     * Set the query result pointer (for builtin bindings).
     *
     * @param resultsPtr query result pointer
     */
    public void setResultsPtr(long resultsPtr) {
        this.resultsPtr = resultsPtr;
    }

    /**
     * An indication that the query result is in json format.
     *
     * @return true, if the query result is in json format
     */
    public boolean isJson() {
        return isJson;
    }

    /**
     * Set the indication that the query result is in json format.
     *
     * @param json true, if the query result is in json format
     */
    public void setJson(boolean json) {
        isJson = json;
    }

    /**
     * An indication that the query result contains rank values.
     *
     * @return true, if query result contains rank values
     */
    public boolean isWithRank() {
        return withRank;
    }

    /**
     * Set the indication that the query result contains rank values.
     *
     * @param withRank true, if query result contains rank values
     */
    public void setWithRank(boolean withRank) {
        this.withRank = withRank;
    }

    /**
     * An indication that the query result contains shard ID values.
     *
     * @return true, if query result contains shard ID values
     */
    public boolean isWithShardID() {
        return withShardID;
    }

    /**
     * Set the indication that the query result contains shard ID values.
     *
     * @param withShardID true, if query result contains shard ID values
     */
    public void setWithShardID(boolean withShardID) {
        this.withShardID = withShardID;
    }

    /**
     * Get the {@link ByteBuffer} with query result data
     *
     * @return the {@link ByteBuffer} with query result data
     */
    public ByteBuffer getBuffer() {
        return buffer;
    }

    /**
     * Set the {@link ByteBuffer} with query result data
     *
     * @param buffer byte buffer with query result data
     */
    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    /**
     * An indication that the query result contains item ids.
     *
     * @return true, if the query result contains item ids
     */
    public boolean isWithItemId() {
        return withItemId;
    }

    /**
     * Set the indication that the query result contains item ids.
     *
     * @param withItemId true, if query result contains contains item ids
     */
    public void setWithItemId(boolean withItemId) {
        this.withItemId = withItemId;
    }

    /**
     * An indication that the query result contains namespace ids.
     *
     * @return true, if the query result contains namespace id
     */
    public boolean isWithNsId() {
        return withNsId;
    }

    /**
     * Set the indication that the query result contains namespace ids.
     *
     * @param withNsId true, if query result contains contains namespace ids
     */
    public void setWithNsId(boolean withNsId) {
        this.withNsId = withNsId;
    }

    /**
     * An indication that the query result contains payload types.
     *
     * @return true, if the query result contains payload types
     */
    public boolean isWithPayloadTypes() {
        return withPayloadTypes;
    }

    /**
     * Set the indication that the query result contains namespace ids.
     *
     * @param withPayloadTypes true, if query result contains contains payload types
     */
    public void setWithPayloadTypes(boolean withPayloadTypes) {
        this.withPayloadTypes = withPayloadTypes;
    }

    /**
     * An indication that the query result contains result pointer (for builtin bindings).
     *
     * @return true, if the query result contains result pointer
     */
    public boolean isWithResultsPtr() {
        return withResultsPtr;
    }

    /**
     * Set the indication that the query result result pointer (for builtin bindings).
     *
     * @param withResultsPtr true, if query result contains contains result pointer.
     */
    public void setWithResultsPtr(boolean withResultsPtr) {
        this.withResultsPtr = withResultsPtr;
    }

    /**
     * Get the current query result payload types.
     *
     * @return list of {@link PayloadType}
     */
    public List<PayloadType> getPayloadTypes() {
        return payloadTypes;
    }

    /**
     * Set the current query result payload types.
     *
     * @param payloadTypes query result payload types
     */
    public void setPayloadTypes(List<PayloadType> payloadTypes) {
        this.payloadTypes = payloadTypes;
    }

    /**
     * An indication that the query result contains joined data.
     *
     * @return true, if the query result contains joined data
     */
    public boolean isWithJoined() {
        return withJoined;
    }

    /**
     * Set the indication that the query result contains joined data.
     *
     * @param withJoined true, if query result contains joined data
     */
    public void setWithJoined(boolean withJoined) {
        this.withJoined = withJoined;
    }

    /**
     * Get the current query result aggregation results.
     *
     * @return list of {@link AggregationResult}
     */
    public List<AggregationResult> getAggResults() {
        return aggResults;
    }

    /**
     * Set the current query result aggregation results.
     *
     * @param aggResults query aggregation results
     */
    public void setAggResults(List<AggregationResult> aggResults) {
        this.aggResults = aggResults;
    }

    /**
     * Get the global shard ID value for query result.
     *
     * @return global shard ID
     */
    public int getShardID() {
        return shardID;
    }

    /**
     * Set the global shard ID value for query result.
     *
     * @param shardID shard ID value
     */
    public void setShardID(int shardID) {
        this.shardID = shardID;
    }

    /**
     * Get version of the sharding config.
     *
     * @return sharding config version
     */
    public long getShardingVersion() {
        return shardingVersion;
    }

    /**
     * Get version of the sharding config.
     *
     * @param shardingVersion sharding config version
     */
    public void setShardingVersion(long shardingVersion) {
        this.shardingVersion = shardingVersion;
    }
}
