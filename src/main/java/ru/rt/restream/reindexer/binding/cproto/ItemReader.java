package ru.rt.restream.reindexer.binding.cproto;

/**
 * Reads items from {@link ByteBuffer}.
 */
public interface ItemReader<T> {

    /**
     * Reads item from {@link ByteBuffer}.
     *
     * @param buffer a buffer to read item from
     * @return read item
     */
    T readItem(ByteBuffer buffer);

}
