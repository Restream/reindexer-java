package ru.rt.restream.reindexer;

import java.util.Iterator;

/**
 * This interface is used by iterators that use releasable resources during iteration.
 *
 * @param <E> the type of elements returned by this iterator
 * @see Iterator
 * @see AutoCloseable
 */
public interface CloseableIterator<E> extends Iterator<E>, AutoCloseable {

}
