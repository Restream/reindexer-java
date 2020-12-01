package ru.rt.restream.reindexer.binding.cproto.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.binding.cproto.ItemReader;

import java.nio.charset.StandardCharsets;

/**
 * An implementation of {@link ItemReader<T>} that reads items encoded in JSON format.
 */
public class JsonItemReader<T> implements ItemReader<T> {

    private final Class<T> itemClass;

    private final Gson gson = new GsonBuilder()
            .create();

    private final boolean withRank;

    public JsonItemReader(Class<T> itemClass, boolean withRank) {
        this.itemClass = itemClass;
        this.withRank = withRank;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T readItem(ByteBuffer buffer) {
        if (withRank) {
            //used for full-text search
            long rank = buffer.getVarUInt();
        }
        int length = (int) buffer.getUInt32();
        byte[] bytes = buffer.getBytes(length);
        String json = new String(bytes, StandardCharsets.UTF_8);
        return gson.fromJson(json, itemClass);
    }

}
