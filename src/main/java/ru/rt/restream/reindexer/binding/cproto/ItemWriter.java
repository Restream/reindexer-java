package ru.rt.restream.reindexer.binding.cproto;

/**
 * Writes items to {@link ByteBuffer}.
 */
public interface ItemWriter<T> {

    /**
     * Write item to {@link ByteBuffer}.
     *
     * @param buffer a buffer to write to
     * @param item   an item to write
     */
    void writeItem(ByteBuffer buffer, T item);

}
