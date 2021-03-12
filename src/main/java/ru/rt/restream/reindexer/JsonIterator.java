package ru.rt.restream.reindexer;

import ru.rt.restream.reindexer.binding.AggregationResult;
import ru.rt.restream.reindexer.binding.RequestContext;

import java.util.List;

// json не поддерживает fetch queries ???
public class JsonIterator implements CloseableIterator{
    public <T> JsonIterator(ReindexerNamespace<T> namespace, RequestContext requestContext, Query tQuery) {

    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public List<AggregationResult> aggResults() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public byte[] next() {
        return null;
    }
}
