package ru.rt.restream.reindexer.binding.cproto.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.AllArgsConstructor;
import ru.rt.restream.reindexer.Namespace;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.binding.cproto.ItemReader;

import java.nio.charset.StandardCharsets;

/**
 * An implementation of {@link ItemReader<T>} which reads items encoded in JSON format.
 */
@AllArgsConstructor
public class JsonItemReader<T> implements ItemReader<T> {

    private final Namespace<T> namespace;

    private final Gson gson = new GsonBuilder()
            .registerTypeAdapterFactory(IndexTypeAdapterFactory.INSTANCE)
            .create();

    /**
     * {@inheritDoc}
     */
    @Override
    public T readItem(ByteBuffer buffer) {
        int length = (int) buffer.getUInt32();
        byte[] bytes = buffer.getBytes(length);
        String json = new String(bytes, StandardCharsets.UTF_8);
        return gson.fromJson(json, namespace.getItemClass());
    }

}
