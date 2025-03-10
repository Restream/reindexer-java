/*
 * Copyright 2020 Restream
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.rt.restream.reindexer.annotations;

import ru.rt.restream.reindexer.CollateMode;
import ru.rt.restream.reindexer.EnumType;
import ru.rt.restream.reindexer.FieldType;
import ru.rt.restream.reindexer.IndexType;
import ru.rt.restream.reindexer.ReindexScanner;
import ru.rt.restream.reindexer.ReindexerIndex;
import ru.rt.restream.reindexer.exceptions.IndexConflictException;
import ru.rt.restream.reindexer.fulltext.FullTextConfig;
import ru.rt.restream.reindexer.util.BeanPropertyUtils;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static ru.rt.restream.reindexer.FieldType.BOOL;
import static ru.rt.restream.reindexer.FieldType.COMPOSITE;
import static ru.rt.restream.reindexer.FieldType.DOUBLE;
import static ru.rt.restream.reindexer.FieldType.FLOAT;
import static ru.rt.restream.reindexer.FieldType.INT;
import static ru.rt.restream.reindexer.FieldType.INT64;
import static ru.rt.restream.reindexer.FieldType.STRING;
import static ru.rt.restream.reindexer.FieldType.UUID;

/**
 * {@inheritDoc}
 * Collects index configuration from an item class annotations.
 */
public class ReindexAnnotationScanner implements ReindexScanner {

    private static final Map<Class<?>, FieldType> MAPPED_TYPES;

    static {
        MAPPED_TYPES = new HashMap<>();
        //Booleans
        MAPPED_TYPES.put(boolean.class, BOOL);
        MAPPED_TYPES.put(Boolean.class, BOOL);
        //Integers
        MAPPED_TYPES.put(byte.class, INT);
        MAPPED_TYPES.put(Byte.class, INT);
        MAPPED_TYPES.put(short.class, INT);
        MAPPED_TYPES.put(Short.class, INT);
        MAPPED_TYPES.put(int.class, INT);
        MAPPED_TYPES.put(Integer.class, INT);
        //Long
        MAPPED_TYPES.put(long.class, INT64);
        MAPPED_TYPES.put(Long.class, INT64);
        //Floats
        MAPPED_TYPES.put(float.class, FLOAT);
        MAPPED_TYPES.put(Float.class, FLOAT);
        MAPPED_TYPES.put(double.class, DOUBLE);
        MAPPED_TYPES.put(Double.class, DOUBLE);
        //String
        MAPPED_TYPES.put(String.class, STRING);
        MAPPED_TYPES.put(char.class, STRING);
        MAPPED_TYPES.put(Character.class, STRING);
        //UUID
        MAPPED_TYPES.put(UUID.class, UUID);
    }

    @Override
    public List<ReindexerIndex> parseIndexes(Class<?> itemClass) {
        return parseIndexes(itemClass, false, "", "", new HashMap<>());
    }

    List<ReindexerIndex> parseIndexes(Class<?> itemClass, boolean subArray, String reindexBasePath, String jsonBasePath,
                                      Map<String, List<Integer>> joined) {
        if (reindexBasePath.length() != 0 && !reindexBasePath.endsWith(".")) {
            reindexBasePath = reindexBasePath + ".";
        }

        if (jsonBasePath.length() != 0 && !jsonBasePath.endsWith(".")) {
            jsonBasePath = jsonBasePath + ".";
        }

        List<ReindexerIndex> indexes = new ArrayList<>();
        Map<String, ReindexerIndex> nameToIndexMap = new HashMap<>();
        List<Field> fields = BeanPropertyUtils.getInheritedFields(itemClass);
        for (Field field : fields) {
            Reindex reindex = field.getAnnotation(Reindex.class);
            // todo remove validate after release of support of no index UUID fields
            validateUuidFieldHasIndex(itemClass, field, reindex);
            if (reindex == null || "-".equals(reindex.name()) || field.isAnnotationPresent(Transient.class)) {
                continue;
            }

            String reindexPath = reindexBasePath + reindex.name();
            Json json = field.getAnnotation(Json.class);
            String jsonPath = jsonBasePath + (json == null ? field.getName() : json.value());
            FieldInfo fieldInfo = getFieldInfo(field);

            // If at least one array (collection) is encountered on a nested path for some field,
            // or the field itself is an array,
            // or the field is appendable
            // then the index on it must also be an array.
            if (subArray || reindex.isAppendable()) {
                fieldInfo.isArray = true;
            }
            if (COMPOSITE == fieldInfo.fieldType && !fieldInfo.isArray) {
                List<ReindexerIndex> nested = parseIndexes(field.getType(), subArray, reindexPath, jsonPath, joined);
                indexes.addAll(nested);
            } else if ((fieldInfo.isArray) && fieldInfo.componentType != null
                    && getFieldTypeByClass(fieldInfo.componentType) == COMPOSITE) {
                List<ReindexerIndex> nested = parseIndexes(fieldInfo.componentType, true, reindexPath, jsonPath, joined);
                indexes.addAll(nested);
            } else {
                String collate = reindex.collate();
                CollateMode collateMode = getCollateMode(collate);
                String sortOrder = getSortOrder(collateMode, collate);
                String precept = null;
                if (field.isAnnotationPresent(Serial.class)) {
                    if (!fieldInfo.isInt()) {
                        throw new IllegalStateException("@Serial is allowed only for int types (i.e INT, INT64)");
                    }
                    precept = field.getName() + "=serial()";
                }
                FullTextConfig fullTextConfig = getFullTextConfig(field, reindex.type());
                validateUuidIndexHasTypeUuidOrString(reindex, fieldInfo);
                boolean isUuid = reindex.isUuid() || fieldInfo.fieldType == UUID;
                IndexType indexType = (isUuid && reindex.type() == IndexType.DEFAULT) ? IndexType.HASH : reindex.type();
                FieldType fieldType = isUuid ? UUID : fieldInfo.fieldType;
                ReindexerIndex index = createIndex(reindexPath, Collections.singletonList(jsonPath), indexType,
                        fieldType, reindex.isDense(), reindex.isSparse(), reindex.isPrimaryKey(),
                        fieldInfo.isArray, collateMode, sortOrder, precept, fullTextConfig, isUuid, reindex.isAppendable());

                ReindexerIndex sameNameIndex = nameToIndexMap.get(reindex.name());
                if (sameNameIndex == null) {
                    indexes.add(index);
                    nameToIndexMap.put(reindex.name(), index);
                } else if (sameNameIndex.isAppendable() && index.isAppendable() && sameNameIndex.equals(index)) {
                    sameNameIndex.getJsonPaths().add(jsonPath);
                } else {
                    String errorMessage;
                    if (reindex.isAppendable() && sameNameIndex.isAppendable()) {
                        errorMessage = String.format("Appendable indexes with name '%s' in class '%s' " +
                                        "must have the same configuration",
                                reindex.name(), itemClass.getName());
                    } else if (reindex.isAppendable() ^ sameNameIndex.isAppendable()) {
                        errorMessage = String.format("Multiple indexes with name '%s' in class '%s'," +
                                        " but at least one of them is not marked as appendable",
                                reindex.name(), itemClass.getName());
                    } else {
                        errorMessage = String.format(
                                "Non-unique index name in class %s: %s",
                                itemClass.getName(), reindex.name());
                    }
                    throw new IndexConflictException(errorMessage);
                }
            }
        }

        Reindex[] composites = itemClass.getAnnotationsByType(Reindex.class);
        for (Reindex composite : composites) {
            String collate = composite.collate();
            CollateMode collateMode = getCollateMode(collate);
            String sortOrder = getSortOrder(collateMode, collate);
            ReindexerIndex compositeIndex = createIndex(String.join("+", composite.subIndexes()),
                    Arrays.asList(composite.subIndexes()), composite.type(), COMPOSITE, composite.isDense(),
                    composite.isSparse(), composite.isPrimaryKey(), false, collateMode, sortOrder, null, null, false, false);
            indexes.add(compositeIndex);
        }

        return indexes;
    }

    private void validateUuidFieldHasIndex(Class<?> itemClass, Field field, Reindex reindex) {
        if (reindex == null && field.getType() == UUID.class) {
            throw new RuntimeException(String.format("Field %s.%s has type UUID so it must have annotation Reindex ",
                    itemClass.getSimpleName(), field.getName()));
        }
    }

    private void validateUuidIndexHasTypeUuidOrString(Reindex index, FieldInfo fieldInfo) {
        if (index.isUuid() && fieldInfo.fieldType != UUID && fieldInfo.fieldType != STRING) {
            throw new RuntimeException("UUID index can only be for UUID or String fields");
        }
    }

    private FullTextConfig getFullTextConfig(Field field, IndexType type) {
        if (type != IndexType.TEXT || !field.isAnnotationPresent(FullText.class)) {
            return null;
        }
        return FullTextConfig.of(field.getAnnotation(FullText.class));
    }

    private String getSortOrder(CollateMode collateMode, String collate) {
        if (CollateMode.CUSTOM == collateMode) {
            return collate;
        }

        return "";
    }

    private CollateMode getCollateMode(String collate) {
        return Arrays.stream(CollateMode.values())
                .filter(cm -> cm.getName().equals(collate))
                .findFirst()
                .orElse(CollateMode.CUSTOM);
    }

    private ReindexerIndex createIndex(String reindexPath, List<String> jsonPath, IndexType indexType,
                                       FieldType fieldType, boolean isDense, boolean isSparse, boolean isPk,
                                       boolean isArray, CollateMode collateMode, String sortOrder, String precept,
                                       FullTextConfig textConfig, boolean isUuid, boolean isAppendable) {
        ReindexerIndex index = new ReindexerIndex();
        index.setName(reindexPath);
        index.setSortOrder(sortOrder);
        index.setCollateMode(collateMode);
        index.setJsonPaths(new ArrayList<>(jsonPath));
        index.setDense(isDense);
        index.setSparse(isSparse);
        index.setPk(isPk);
        index.setArray(isArray);
        index.setIndexType(indexType);
        index.setFieldType(fieldType);
        index.setPrecept(precept);
        index.setFullTextConfig(textConfig);
        index.setUuid(isUuid);
        index.setAppendable(isAppendable);
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
                Class<?> componentType = (Class<?>) typeArgument;
                fieldType = getFieldTypeByClass(componentType);
                fieldInfo.componentType = componentType;
            }
        } else if (Enum.class.isAssignableFrom(type)) {
            Enumerated enumerated = field.getAnnotation(Enumerated.class);
            fieldType = enumerated != null && enumerated.value() == EnumType.STRING ? STRING : INT;
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
        return MAPPED_TYPES.getOrDefault(type, COMPOSITE);
    }

    private static class FieldInfo {
        private FieldType fieldType;
        private boolean isArray;
        private Class<?> componentType;

        private boolean isInt() {
            return fieldType == INT || fieldType == INT64;
        }
    }

}
