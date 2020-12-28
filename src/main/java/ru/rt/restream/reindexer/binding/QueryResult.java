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

    private boolean isJson;

    private boolean withRank;

    private boolean withItemId;

    private boolean withNsId;

    private boolean withPayloadTypes;

    private List<PayloadType> payloadTypes;

    /**
     * Encoded query result data.
     */
    private ByteBuffer buffer;

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    public long getQCount() {
        return qCount;
    }

    public void setqCount(long qCount) {
        this.qCount = qCount;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public boolean isJson() {
        return isJson;
    }

    public void setJson(boolean json) {
        isJson = json;
    }

    public boolean isWithRank() {
        return withRank;
    }

    public void setWithRank(boolean withRank) {
        this.withRank = withRank;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public boolean isWithItemId() {
        return withItemId;
    }

    public void setWithItemId(boolean withItemId) {
        this.withItemId = withItemId;
    }

    public boolean isWithNsId() {
        return withNsId;
    }

    public void setWithNsId(boolean withNsId) {
        this.withNsId = withNsId;
    }

    public boolean isWithPayloadTypes() {
        return withPayloadTypes;
    }

    public void setWithPayloadTypes(boolean withPayloadTypes) {
        this.withPayloadTypes = withPayloadTypes;
    }

    public List<PayloadType> getPayloadTypes() {
        return payloadTypes;
    }

    public void setPayloadTypes(List<PayloadType> payloadTypes) {
        this.payloadTypes = payloadTypes;
    }
}
