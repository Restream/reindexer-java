package ru.rt.restream.reindexer.annotations;

import org.apache.commons.lang3.StringUtils;
import ru.rt.restream.reindexer.Index;
import ru.rt.restream.reindexer.Namespace;
import ru.rt.restream.reindexer.NamespaceScanner;
import ru.rt.restream.reindexer.binding.Consts;
import ru.rt.restream.reindexer.exceptions.ReindexerException;
import ru.rt.restream.reindexer.exceptions.UnimplementedException;
import ru.rt.restream.reindexer.util.BeanPropertyUtils;
import ru.rt.restream.reindexer.util.Pair;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ru.rt.restream.reindexer.Namespace.DEFAULT_CREATE_IF_MISSING;
import static ru.rt.restream.reindexer.Namespace.DEFAULT_DISABLE_OBJ_CACHE;
import static ru.rt.restream.reindexer.Namespace.DEFAULT_DROP_ON_FILE_FORMAT_ERROR;
import static ru.rt.restream.reindexer.Namespace.DEFAULT_DROP_ON_INDEX_CONFLICT;
import static ru.rt.restream.reindexer.Namespace.DEFAULT_ENABLE_STORAGE;
import static ru.rt.restream.reindexer.Namespace.DEFAULT_OBJ_CACHE_ITEMS_COUNT;

/**
 * Scans item class for reindexer annotations {@link ru.rt.restream.reindexer.annotations.Namespace} and {@link
 * Reindex}.
 */
public class NamespaceAnnotationScanner implements NamespaceScanner {

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Namespace<T> scanClassNamespace(String namespaceName, Class<T> itemClass) {
        final Class<ru.rt.restream.reindexer.annotations.Namespace> annotation
                = ru.rt.restream.reindexer.annotations.Namespace.class;

        ru.rt.restream.reindexer.annotations.Namespace[] namespaces = itemClass.getAnnotationsByType(annotation);

        Namespace.NamespaceBuilder<T> configuration;
        if (namespaces.length == 0) {
            configuration = defaultConfiguration(namespaceName, itemClass);
        } else {
            ru.rt.restream.reindexer.annotations.Namespace namespace = Stream.of(namespaces)
                    .filter(ns -> ns.name().equals(namespaceName) || StringUtils.isBlank(ns.name()))
                    .findFirst()
                    .orElseThrow(() -> {
                        String msg = String.format("No namespace '%s' defined on class '%s'", namespaceName,
                                itemClass.getName());
                        return new ReindexerException(msg);
                    });

            configuration = fromAnnotation(namespaceName, namespace, itemClass);
        }

        List<Index> indexConfigurations = parseIndexes(namespaceName, itemClass);

        return configuration
                .indices(indexConfigurations)
                .build();
    }

    private List<Index> parseIndexes(String namespaceName, Class<?> itemClass) {
        return BeanPropertyUtils.getInheritedFields(itemClass).stream()
                .filter(field -> field.getAnnotationsByType(Reindex.class).length > 0)
                .map(field -> toFieldPair(namespaceName, field))
                .filter(this::indexNotIgnored)
                .map(this::toIndexConfiguration)
                .collect(Collectors.toList());
    }

    private boolean indexNotIgnored(Pair<Reindex, Field> pair) {
        String indexName = pair.getFirst().name();
        return !indexName.equals("-");
    }

    private Index toIndexConfiguration(Pair<Reindex, Field> pair) {
        Reindex reindex = pair.getFirst();
        if (reindex == null) {
            throw new UnimplementedException();
        } else {
            return getIndexByAnnotation(pair);
        }
    }

    private Index getIndexByAnnotation(Pair<Reindex, Field> pair) {
        Reindex reindex = pair.getFirst();
        Field field = pair.getSecond();

        String indexName = StringUtils.isNotBlank(reindex.name()) ? reindex.name() : field.getName();

        String fieldType = getFieldType(field.getGenericType());
        //TODO: collateMode, sortOrderLetters := parseCollate(&idxSettings)
        String collateMode = getCollateMode(Consts.COLLATE_NONE);
        return Index.builder()
                .indexFieldPair(Pair.<String, String>builder()
                        .first(indexName)
                        .second(field.getName()).build())
                .jsonPaths(Collections.singletonList(indexName))
                .indexType(reindex.type())
                .fieldType(fieldType)
                .options(new HashSet<>(Arrays.asList(reindex.options())))
                .collateMode(collateMode)
                .sortOrder("")
                .build();
    }

    private String getCollateMode(int collate) {
        String collateMode = "";
        switch (collate) {
            case Consts.COLLATE_ASCII:
                collateMode = "ascii";
                break;
            case Consts.COLLATE_UTF_8:
                collateMode = "utf8";
                break;
            case Consts.COLLATE_NUMERIC:
                collateMode = "numeric";
                break;
            case Consts.COLLATE_CUSTOM:
                collateMode = "custom";
                break;
        }

        return collateMode;
    }

    private String getFieldType(Type genericType) {
        switch (genericType.getTypeName()) {
            case "boolean":
            case "java.lang.Boolean":
                return "bool";
            case "byte":
            case "java.lang.Byte":
            case "short":
            case "java.lang.Short":
            case "int":
            case "java.lang.Integer":
                return "int";
            case "java.lang.Long":
                return "int64";
            case "java.lang.String":
                return "string";
            case "java.lang.Double":
            case "java.lang.Float":
                return "double";
            default:
                throw new ReindexerException("Invalid generic type");
        }
    }

    private Pair<Reindex, Field> toFieldPair(String namespaceName, Field field) {
        Comparator<Reindex> compareByNamespace = (r1, r2) -> {
            if (namespaceName.equals(r1.nameSpace())) {
                return 1;
            } else if (namespaceName.equals(r2.nameSpace())) {
                return -1;
            } else {
                return 0;
            }
        };

        Reindex reindex = Stream.of(field.getAnnotationsByType(Reindex.class))
                .filter(r -> r.nameSpace().equals(namespaceName) || StringUtils.isBlank(r.nameSpace()))
                .min(compareByNamespace)
                .orElseThrow(UnimplementedException::new);

        return Pair.<Reindex, Field>builder()
                .first(reindex)
                .second(field)
                .build();
    }

    private <T> Namespace.NamespaceBuilder<T> defaultConfiguration(String namespaceName, Class<T> itemClass) {
        return Namespace.<T>builder()
                .name(namespaceName)
                .itemClass(itemClass)
                .enableStorage(DEFAULT_ENABLE_STORAGE)
                .createStorageIfMissing(DEFAULT_CREATE_IF_MISSING)
                .dropStorageOnFileFormatError(DEFAULT_DROP_ON_FILE_FORMAT_ERROR)
                .dropOnIndexConflict(DEFAULT_DROP_ON_INDEX_CONFLICT)
                .disableObjCache(DEFAULT_DISABLE_OBJ_CACHE)
                .objCacheItemsCount(DEFAULT_OBJ_CACHE_ITEMS_COUNT);
    }

    private <T> Namespace.NamespaceBuilder<T> fromAnnotation(String namespaceName,
                                                             ru.rt.restream.reindexer.annotations.Namespace annotation,
                                                             Class<T> itemClass) {
        return Namespace.<T>builder()
                .name(StringUtils.isNotBlank(annotation.name()) ? annotation.name() : namespaceName)
                .itemClass(itemClass)
                .enableStorage(annotation.enableStorage())
                .createStorageIfMissing(annotation.createStorageIfMissing())
                .dropStorageOnFileFormatError(annotation.dropStorageOnFileFormatError())
                .dropOnIndexConflict(annotation.dropOnIndexConflict())
                .disableObjCache(annotation.disableObjCache())
                .objCacheItemsCount(annotation.objCacheItemsCount());
    }

}
