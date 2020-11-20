package ru.rt.restream.reindexer.connector.binding.cproto.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import ru.rt.restream.reindexer.connector.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.connector.binding.cproto.ItemReader;

import java.nio.charset.StandardCharsets;

public class JsonItemReader implements ItemReader {

    private final Gson gson;

    private final ByteBuffer deserializer;

    public JsonItemReader(byte[] data) {
        deserializer = new ByteBuffer(data);
        gson = new GsonBuilder()
                .registerTypeAdapterFactory(IndexTypeAdapterFactory.INSTANCE)
                .create();
    }

    @Override
    public <T> T readItem(Class<T> itemClass) {
        int length = (int) deserializer.readUnsignedInt();
        byte[] bytes = deserializer.readBytes(length);
        String json = new String(bytes, StandardCharsets.UTF_8);
        return gson.fromJson(json, itemClass);
    }

}
