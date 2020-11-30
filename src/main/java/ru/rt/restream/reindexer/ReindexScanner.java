package ru.rt.restream.reindexer;

import java.util.List;

/**
 * Scans item class for reindex item configuration.
 */
public interface ReindexScanner {

    /**
     * Returns list of {@link ReindexerIndex} for the given item class.
     *
     * @param itemClass item class to store within specified namespace
     */
    List<ReindexerIndex> parseIndexes(Class<?> itemClass);

}
