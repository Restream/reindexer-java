package ru.rt.restream.reindexer.binding.cproto.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.AllArgsConstructor;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.binding.cproto.ItemWriter;

import java.nio.charset.StandardCharsets;

/**
 * An implementation of {@link ItemWriter <T>} that encodes items in JSON format.
 */
@AllArgsConstructor
public class JsonItemWriter<T> implements ItemWriter<T> {

    private final Gson gson = new GsonBuilder()
            .create();

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeItem(ByteBuffer buffer, Object item) {
        String json = gson.toJson(item);
        buffer.writeBytes(json.getBytes(StandardCharsets.UTF_8));
    }

}
