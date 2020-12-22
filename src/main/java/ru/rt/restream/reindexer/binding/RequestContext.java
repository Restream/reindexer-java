package ru.rt.restream.reindexer.binding;

/**
 * A request context.
 */
public interface RequestContext {

    /**
     * Returns the current {@link QueryResult}.
     *
     * @return the current {@link QueryResult}
     */
    QueryResult getQueryResult();

    /**
     * Fetches part of the results.
     *
     * @param offset an offset
     * @param limit  a limit
     */
    void fetchResults(int offset, int limit);

    /**
     * Closes the context.
     */
    void close();

}
