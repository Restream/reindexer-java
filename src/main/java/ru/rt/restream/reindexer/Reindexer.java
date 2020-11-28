package ru.rt.restream.reindexer;

import ru.rt.restream.reindexer.annotations.NamespaceAnnotationScanner;
import ru.rt.restream.reindexer.binding.Binding;
import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.binding.cproto.ItemWriter;
import ru.rt.restream.reindexer.binding.cproto.json.JsonItemWriter;
import ru.rt.restream.reindexer.binding.definition.IndexDefinition;
import ru.rt.restream.reindexer.binding.definition.NamespaceDefinition;
import ru.rt.restream.reindexer.exceptions.IndexConflictException;
import ru.rt.restream.reindexer.exceptions.NamespaceExistsException;
import ru.rt.restream.reindexer.util.Pair;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Reindexer {

    private static final int MODE_UPDATE = 0;

    private static final int MODE_INSERT = 1;

    private static final int MODE_UPSERT = 2;

    private static final int MODE_DELETE = 3;

    private final Binding binding;

    private final NamespaceScanner namespaceScanner = new NamespaceAnnotationScanner();

    private final Map<Pair<String, Class<?>>, Namespace<?>> namespaceMap = new ConcurrentHashMap<>();

    Reindexer(Binding binding) {
        this.binding = binding;
    }

    public void close() {
        binding.close();
    }

    public <T> void openNamespace(String name, Class<T> itemClass) {
        Namespace<T> namespace = namespaceScanner.scanClassNamespace(name, itemClass);
        registerNamespace(itemClass, namespace);
        try {
            binding.openNamespace(NamespaceDefinition.fromNamespace(namespace));
            for (Index index : namespace.getIndices()) {
                IndexDefinition indexDefinition = IndexDefinition.fromIndex(index);
                binding.addIndex(name, indexDefinition);
            }
        } catch (IndexConflictException e) {
            if (namespace.isDropOnIndexConflict()) {
                binding.dropNamespace(name);
            } else {
                binding.closeNamespace(name);
            }
        } catch (Exception e) {
            binding.closeNamespace(name);
            throw e;
        }

    }

    public<T> void upsert(String namespaceName, T item) {
        Class<T> itemClass = (Class<T>) item.getClass();
        Namespace<T> namespace = getNamespace(namespaceName, itemClass);
        modifyItem(namespace, item, MODE_UPSERT);
    }

    public <T> Query<T> query(String namespaceName, Class<T> clazz) {
        Namespace<T> namespace = getNamespace(namespaceName, clazz);
        return new Query<>(binding, namespace);
    }

    private <T> Namespace<T> getNamespace(String namespaceName, Class<T> itemClass) {
        Pair<String, Class<?>> key = Pair.<String, Class<?>>builder()
                .first(namespaceName)
                .second(itemClass)
                .build();
        Namespace<?> namespace = namespaceMap.get(key);

        if (namespace.getItemClass() != itemClass) {
            throw new RuntimeException("Wrong namespace item type");
        }
        return (Namespace<T>) namespace;
    }

    private <T> void registerNamespace(Class<T> itemClass, Namespace<T> namespace) {
        Pair<String, Class<?>> key = Pair.<String, Class<?>>builder()
                .first(namespace.getName())
                .second(itemClass)
                .build();
        if (namespaceMap.containsKey(key)) {
            throw new NamespaceExistsException();
        }

        namespaceMap.put(key, namespace);
    }

    private <T> void modifyItem(Namespace<T> namespace, T item, int mode) {
        //TODO: percepts
        String[] percepts = new String[0];
        //TODO: cjson
        int format = Consts.FORMAT_JSON;
        //TODO: stateToken
        int stateToken = 0;

        ByteBuffer buffer = new ByteBuffer();
        buffer.putVarInt64(format);
        ItemWriter<T> itemWriter = new JsonItemWriter<>();
        itemWriter.writeItem(buffer, item);

        binding.modifyItem(namespace.getName(), format, buffer.bytes(), mode, percepts, stateToken);
    }

}

