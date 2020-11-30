package ru.rt.restream.reindexer.annotations;

import ru.rt.restream.reindexer.*;
import ru.rt.restream.reindexer.util.BeanPropertyUtils;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

import static ru.rt.restream.reindexer.FieldType.*;

public class ReindexAnnotationScanner implements ReindexScanner {

    private static final Set<Class<?>> BOOLEAN_TYPES;
    private static final Set<Class<?>> INT_TYPES;
    private static final Set<Class<?>> DOUBLE_TYPES;

    static {
        BOOLEAN_TYPES = new HashSet<>();
        BOOLEAN_TYPES.add(boolean.class);
        BOOLEAN_TYPES.add(Boolean.class);

        INT_TYPES = new HashSet<>();
        INT_TYPES.add(byte.class);
        INT_TYPES.add(Byte.class);
        INT_TYPES.add(short.class);
        INT_TYPES.add(Short.class);
        INT_TYPES.add(int.class);
        INT_TYPES.add(Integer.class);

        DOUBLE_TYPES = new HashSet<>();
        DOUBLE_TYPES.add(float.class);
        DOUBLE_TYPES.add(Float.class);
        DOUBLE_TYPES.add(double.class);
        DOUBLE_TYPES.add(Double.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ReindexerIndex> parseIndexes(Class<?> itemClass) {
        return parseIndexes(itemClass, false, "", new HashMap<>());
    }

    List<ReindexerIndex> parseIndexes(Class<?> itemClass, boolean subArray, String reindexBasePath,
                                      Map<String, int[]> joined) {
        if (reindexBasePath.length() != 0 && !reindexBasePath.endsWith(".")) {
            reindexBasePath = reindexBasePath + ".";
        }

        List<ReindexerIndex> indexes = new ArrayList<>();
        List<Field> fields = BeanPropertyUtils.getInheritedFields(itemClass);
        for (Field field : fields) {
            String reindexPath = reindexBasePath + field.getName();
            Reindex reindex = field.getAnnotation(Reindex.class);
            if (reindex != null && "-".equals(reindex.name())) {
                continue;
            }
            FieldInfo fieldInfo = getFieldInfo(field);
            if (subArray) {
                fieldInfo.isArray = true;
            }
            if (COMPOSITE == fieldInfo.fieldType && !fieldInfo.isArray) {
                List<ReindexerIndex> nested = parseIndexes(field.getType(), true, reindexPath, joined);
                indexes.addAll(nested);
            } else if ((fieldInfo.isArray || subArray) && fieldInfo.componentType!= null
                    && getFieldTypeByClass(fieldInfo.componentType) == COMPOSITE) {
                List<ReindexerIndex> nested = parseIndexes(fieldInfo.componentType, true, reindexPath, joined);
                indexes.addAll(nested);
            } else {
                if (reindex == null) {
                    continue;
                }
                String collate = reindex.collate();
                CollateMode collateMode = Arrays.stream(CollateMode.values())
                        .filter(cm -> cm.getName().equals(collate))
                        .findFirst()
                        .orElse(CollateMode.CUSTOM);
                String sortOrder = "";
                if (collateMode == CollateMode.CUSTOM) {
                    sortOrder = collate;
                }
                ReindexerIndex index = createIndex(reindexPath, Collections.singletonList(reindexPath), reindex.type(),
                        fieldInfo.fieldType, reindex.isDense(), reindex.isSparse(), reindex.isPrimaryKey(),
                        fieldInfo.isArray, collateMode, sortOrder);
                indexes.add(index);
            }
        }

        Reindex[] composites = itemClass.getAnnotationsByType(Reindex.class);
        for (Reindex composite : composites) {
            String collate = composite.collate();
            CollateMode collateMode = Arrays.stream(CollateMode.values())
                    .filter(cm -> cm.getName().equals(collate))
                    .findFirst()
                    .orElse(CollateMode.CUSTOM);
            String sortOrder = "";
            if (collateMode == CollateMode.CUSTOM) {
                sortOrder = collate;
            }
            ReindexerIndex compositeIndex = createIndex(String.join("+", composite.fields()),
                    Arrays.asList(composite.fields()), composite.type(), COMPOSITE, composite.isDense(),
                    composite.isSparse(), composite.isPrimaryKey(), false, collateMode, sortOrder);
            indexes.add(compositeIndex);
        }

        return indexes;
    }

    private ReindexerIndex createIndex(String reindexPath, List<String> jsonPath, IndexType indexType,
                                       FieldType fieldType, boolean isDense, boolean isSparse, boolean isPk,
                                       boolean isArray, CollateMode collateMode, String sortOrder) {
        ReindexerIndex index = new ReindexerIndex();
        index.setName(reindexPath);
        index.setSortOrder(sortOrder);
        index.setCollateMode(collateMode);
        index.setJsonPaths(Collections.singletonList(reindexPath));
        index.setDense(isDense);
        index.setSparse(isSparse);
        index.setPk(isPk);
        index.setArray(isArray);
        index.setJsonPaths(jsonPath);
        index.setIndexType(indexType);
        index.setFieldType(fieldType);
        return index;
    }

    private FieldInfo getFieldInfo(Field field) {
        Class<?> type = field.getType();
        FieldInfo fieldInfo = new FieldInfo();
        fieldInfo.isArray = type.isArray() || Collection.class.isAssignableFrom(type);
        FieldType fieldType = null;
        if (type.isArray()) {
            Class<?> componentType = type.getComponentType();
            fieldType = getFieldTypeByClass(componentType);
            fieldInfo.componentType = componentType;
        } else if (field.getGenericType() instanceof ParameterizedType && fieldInfo.isArray) {
            ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
            Type typeArgument = parameterizedType.getActualTypeArguments()[0];
            if (typeArgument instanceof Class<?>) {
                final Class<?> componentType = (Class<?>) typeArgument;
                fieldType = getFieldTypeByClass(componentType);
                fieldInfo.componentType = componentType;
            }
        } else {
            fieldType = getFieldTypeByClass(type);
        }

        if (fieldType == null) {
            throw new IllegalArgumentException("Unrecognized field type");
        }

        fieldInfo.fieldType = fieldType;
        return fieldInfo;
    }

    private FieldType getFieldTypeByClass(Class<?> type) {
        if (BOOLEAN_TYPES.contains(type)) {
            return BOOL;
        } else if (INT_TYPES.contains(type)) {
            return INT;
        } else if (String.class == type || char.class == type || Character.class == type) {
            return STRING;
        } else if (DOUBLE_TYPES.contains(type)) {
            return DOUBLE;
        } else if (Long.class == type) {
            return INT64;
        } else {
            return COMPOSITE;
        }
    }

    private class FieldInfo {
        private FieldType fieldType;
        private boolean isArray;
        private Class<?> componentType;
    }

}
