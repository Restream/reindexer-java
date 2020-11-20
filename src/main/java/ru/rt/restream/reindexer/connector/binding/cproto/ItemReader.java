package ru.rt.restream.reindexer.connector.binding.cproto;

public interface ItemReader {

    <T> T readItem(Class<T> itemClass);

}
