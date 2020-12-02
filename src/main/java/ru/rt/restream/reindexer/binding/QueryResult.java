package ru.rt.restream.reindexer.binding;

import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;

/**
 * Result of a query.
 */
public class QueryResult {

    /**
     * Used to fetch query data.
     */
    private int requestId;

    private long totalCount;

    private long qCount;

    private long count;

    private boolean isJson;

    private boolean withRank;

    /**
     * Encoded query result data.
     */
    private ByteBuffer buffer;

    public int getRequestId() {
        return requestId;
    }

    public void setRequestId(int requestId) {
        this.requestId = requestId;
    }

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
}
