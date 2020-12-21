package ru.rt.restream.reindexer;

import ru.rt.restream.reindexer.annotations.ReindexAnnotationScanner;
import ru.rt.restream.reindexer.binding.Binding;
import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.binding.cproto.ItemWriter;
import ru.rt.restream.reindexer.binding.cproto.json.JsonItemWriter;
import ru.rt.restream.reindexer.binding.definition.IndexDefinition;
import ru.rt.restream.reindexer.binding.definition.NamespaceDefinition;
import ru.rt.restream.reindexer.binding.option.NamespaceOptions;
import ru.rt.restream.reindexer.exceptions.IndexConflictException;
import ru.rt.restream.reindexer.exceptions.NamespaceExistsException;
import ru.rt.restream.reindexer.util.Pair;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Reindexer {

    static final int MODE_UPDATE = 0;

    static final int MODE_INSERT = 1;

    static final int MODE_UPSERT = 2;

    static final int MODE_DELETE = 3;

    private final Binding binding;

    private final ExecutorService executor;

    private final ReindexScanner reindexScanner = new ReindexAnnotationScanner();

    private final Map<Pair<String, Class<?>>, ReindexerNamespace<?>> namespaceMap = new ConcurrentHashMap<>();

    Reindexer(Binding binding, int threadPoolSize) {
        this.binding = binding;
        this.executor = Executors.newFixedThreadPool(threadPoolSize);
    }

    public void close() {
        binding.close();
        executor.shutdown();
    }

    public <T> void openNamespace(String name, NamespaceOptions options, Class<T> itemClass) {
        ReindexerNamespace<T> namespace = ReindexerNamespace.<T>builder()
                .name(name)
                .itemClass(Objects.requireNonNull(itemClass))
                .enableStorage(options.isEnableStorage())
                .createStorageIfMissing(options.isCreateStorageIfMissing())
                .disableObjCache(options.isDisableObjCache())
                .dropOnIndexConflict(options.isDropOnIndexesConflict())
                .dropStorageOnFileFormatError(options.isDropOnFileFormatError())
                .objCacheItemsCount(options.getObjCacheItemsCount())
                .indexes(reindexScanner.parseIndexes(itemClass))
                .build();

        registerNamespace(itemClass, namespace);
        try {
            binding.openNamespace(NamespaceDefinition.fromNamespace(namespace));
            for (ReindexerIndex index : namespace.getIndexes()) {
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

    /**
     * Inserts the given item data.
     *
     * @param <T>           the item type
     * @param namespaceName the namespace name
     * @param item          the item data
     */
    public <T> void insert(String namespaceName, T item) {
        modifyItem(namespaceName, item, MODE_INSERT);
    }

    /**
     * Updates the given item data.
     *
     * @param <T>           the item type
     * @param namespaceName the namespace name
     * @param item          the item data
     */
    public <T> void update(String namespaceName, T item) {
        modifyItem(namespaceName, item, MODE_UPDATE);
    }

    /**
     * Inserts or updates the given item data.
     *
     * @param <T>           the item type
     * @param namespaceName the namespace name
     * @param item          the item data
     */
    public <T> void upsert(String namespaceName, T item) {
        modifyItem(namespaceName, item, MODE_UPSERT);
    }

    /**
     * Deletes the given item data.
     *
     * @param <T>           the item type
     * @param namespaceName the namespace name
     * @param item          the item data
     */
    public <T> void delete(String namespaceName, T item) {
        modifyItem(namespaceName, item, MODE_DELETE);
    }

    public <T> Transaction<T> beginTransaction(String namespaceName, Class<T> clazz) {
        ReindexerNamespace<T> namespace = getNamespace(namespaceName, clazz);
        Transaction<T> transaction = new Transaction<>(namespace, binding, executor);
        transaction.start();
        return transaction;
    }

    public <T> Query<T> query(String namespaceName, Class<T> clazz) {
        ReindexerNamespace<T> namespace = getNamespace(namespaceName, clazz);
        return new Query<>(binding, namespace, null);
    }

    private <T> ReindexerNamespace<T> getNamespace(String namespaceName, Class<T> itemClass) {
        Pair<String, Class<?>> key = new Pair<>(namespaceName, itemClass);
        ReindexerNamespace<?> namespace = namespaceMap.get(key);
        if (namespace == null) {
            String msg = String.format("Namespace '%s' is not exists.", namespaceName);
            throw new IllegalArgumentException(msg);
        }

        if (namespace.getItemClass() != itemClass) {
            throw new RuntimeException("Wrong namespace item type");
        }
        return (ReindexerNamespace<T>) namespace;
    }

    private <T> void registerNamespace(Class<T> itemClass, ReindexerNamespace<T> namespace) {
        Pair<String, Class<?>> key = new Pair<>(namespace.getName(), itemClass);
        if (namespaceMap.containsKey(key)) {
            throw new NamespaceExistsException();
        }

        namespaceMap.put(key, namespace);
    }

    @SuppressWarnings("unchecked")
    private <T> void modifyItem(String namespaceName, T item, int mode) {
        Class<T> itemClass = (Class<T>) item.getClass();
        ReindexerNamespace<T> namespace = getNamespace(namespaceName, itemClass);
        String[] percepts = namespace.getPrecepts();
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

