package ru.rt.restream.reindexer.binding.cproto.json;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.GsonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import ru.rt.restream.reindexer.annotations.Reindex;
import ru.rt.restream.reindexer.util.JavaBeanUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Allow to process {@link Reindex} annotations during deserialization of reindexer items.
 */
public class IndexTypeAdapterFactory implements TypeAdapterFactory {

    public static final TypeAdapterFactory INSTANCE = new IndexTypeAdapterFactory();

    @Override
    public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> typeToken) {
        TypeAdapter<T> delegateAdapter = gson.getDelegateAdapter(this, typeToken);
        Class<? super T> itemType = typeToken.getRawType();
        List<FieldInfo> fieldInfos = of(itemType);
        return fieldInfos.isEmpty()
                ? delegateAdapter
                : new IndexTypeAdapter<>(gson, delegateAdapter, gson.getAdapter(JsonElement.class), fieldInfos);
    }

    private static final class IndexTypeAdapter<T> extends TypeAdapter<T> {

        private final Gson gson;
        private final TypeAdapter<T> delegateAdapter;
        private final TypeAdapter<JsonElement> jsonElementTypeAdapter;
        private final List<FieldInfo> fieldInfos;

        private IndexTypeAdapter(Gson gson, TypeAdapter<T> delegateAdapter,
                                 TypeAdapter<JsonElement> jsonElementTypeAdapter, List<FieldInfo> fieldInfos) {
            this.gson = gson;
            this.delegateAdapter = delegateAdapter;
            this.jsonElementTypeAdapter = jsonElementTypeAdapter;
            this.fieldInfos = fieldInfos;

            JsonProvider jsonProvider = new GsonJsonProvider(gson);
            MappingProvider gsonMappingProvider = new GsonMappingProvider(gson);
            Configuration.setDefaults(new Configuration.Defaults() {

                @Override
                public JsonProvider jsonProvider() {
                    return jsonProvider;
                }

                @Override
                public MappingProvider mappingProvider() {
                    return gsonMappingProvider;
                }

                @Override
                public Set<Option> options() {
                    return EnumSet.noneOf(Option.class);
                }
            });
        }

        @Override
        public void write(final JsonWriter out, final T value) throws IOException {
            delegateAdapter.write(out, value);
        }

        @Override
        public T read(final JsonReader in) {
            try {
                final JsonElement outerJsonElement = jsonElementTypeAdapter.read(in).getAsJsonObject();
                final T item = delegateAdapter.fromJsonTree(outerJsonElement);
                for (FieldInfo fieldInfo : fieldInfos) {
                    JsonElement jsonValue = fieldInfo.jsonPath.read(outerJsonElement);
                    Object value = gson.fromJson(jsonValue, fieldInfo.fieldType);
                    JavaBeanUtils.setProperty(item, fieldInfo.fieldName, value);
                }
                return item;
            } catch (Exception e) {
                throw new RuntimeException(e.getLocalizedMessage(), e);
            }
        }

    }

    @AllArgsConstructor
    private static class FieldInfo {
        private final String fieldName;
        private final Type fieldType;
        private final JsonPath jsonPath;
    }

    private static List<FieldInfo> of(final Class<?> clazz) {
        List<FieldInfo> fieldInfos = new ArrayList<>();
        for (final Field field : clazz.getDeclaredFields()) {
            Reindex reindex = field.getAnnotation(Reindex.class);
            if (reindex != null) {
                String indexName = StringUtils.isNotBlank(reindex.name()) ? reindex.name() : field.getName();
                fieldInfos.add(new FieldInfo(field.getName(), field.getType(), JsonPath.compile(indexName)));
            }
        }
        return fieldInfos;
    }

}
