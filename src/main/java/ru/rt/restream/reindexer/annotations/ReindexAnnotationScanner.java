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
import ru.rt.restream.reindexer.convert.FieldConverter;
import ru.rt.restream.reindexer.convert.util.ConversionUtils;
import ru.rt.restream.reindexer.convert.FieldConverterRegistryFactory;
import ru.rt.restream.reindexer.exceptions.IndexConflictException;
import ru.rt.restream.reindexer.fulltext.FullTextConfig;
import ru.rt.restream.reindexer.util.BeanPropertyUtils;
import ru.rt.restream.reindexer.convert.util.ResolvableType;
import ru.rt.restream.reindexer.util.Pair;
import ru.rt.restream.reindexer.vector.HnswConfig;
import ru.rt.restream.reindexer.vector.HnswConfigs;
import ru.rt.restream.reindexer.vector.IvfConfig;
import ru.rt.restream.reindexer.vector.IvfConfigs;
import ru.rt.restream.reindexer.vector.VecBfConfig;
import ru.rt.restream.reindexer.vector.VecBfConfigs;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static ru.rt.restream.reindexer.FieldType.BOOL;
import static ru.rt.restream.reindexer.FieldType.COMPOSITE;
import static ru.rt.restream.reindexer.FieldType.DOUBLE;
import static ru.rt.restream.reindexer.FieldType.FLOAT;
import static ru.rt.restream.reindexer.FieldType.FLOAT_VECTOR;
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
                IndexType indexType = specifyDefaultIndexType(reindex, field, fieldInfo);
                validateVectorIndexes(indexType, field, fieldInfo);
                if (indexType.isVectorIndex()) {
                    fieldInfo.isArray = false;
                    fieldInfo.fieldType = FLOAT_VECTOR;
                }
                FieldType fieldType = isUuid ? UUID : fieldInfo.fieldType;
                HnswConfig hnswConfig = getHnswConfig(field, indexType);
                IvfConfig ivfConfig = getIvfConfig(field, indexType);
                VecBfConfig vecBfConfig = getVecBfConfig(field, indexType);

                ReindexerIndex index = createIndex(reindexPath, Collections.singletonList(jsonPath), indexType,
                        fieldType, reindex.isDense(), reindex.isSparse(), reindex.isPrimaryKey(),
                        fieldInfo.isArray, collateMode, sortOrder, precept, fullTextConfig, isUuid, reindex.isAppendable(),
                        reindex.isNoColumn(), hnswConfig, ivfConfig, vecBfConfig);

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
                    composite.isSparse(), composite.isPrimaryKey(), false, collateMode, sortOrder, null, null, false,
                    false, composite.isNoColumn(), null, null, null);
            indexes.add(compositeIndex);
        }

        return indexes;
    }

    private IndexType specifyDefaultIndexType(Reindex reindex, Field field, FieldInfo fieldInfo) {
        if (reindex.type() != IndexType.DEFAULT) {
            return reindex.type();
        }

        if (reindex.isUuid() || fieldInfo.fieldType == UUID) {
            return IndexType.HASH;
        }

        if (fieldInfo.isFloatVector) {
            if (field.isAnnotationPresent(Hnsw.class)) {
                return IndexType.HNSW;
            } else if (field.isAnnotationPresent(Ivf.class)) {
                return IndexType.IVF;
            } else if (field.isAnnotationPresent(VecBf.class)) {
                return IndexType.VEC_BF;
            }
        }
        return IndexType.DEFAULT;
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

    private void validateVectorIndexes(IndexType indexType, Field field, FieldInfo fieldInfo) {
        if (!indexType.isVectorIndex()) {
            return;
        }
        // for vector indices
        if (!fieldInfo.isFloatVector) {
            throw new RuntimeException("Only a float array field can have vector index");
        }
        Class<? extends Annotation> aClass;
        switch (indexType) {
            case HNSW:
                aClass = Hnsw.class;
                break;
            case IVF:
                aClass = Ivf.class;
                break;
            case VEC_BF:
                aClass = VecBf.class;
                break;
            default:
                throw new RuntimeException("Unknown vector index type: " + indexType);
        }
        if (!field.isAnnotationPresent(aClass)) {
            String errorMessage = String.format("Vector index %s must have annotation @%s", indexType, aClass.getSimpleName());
            throw new RuntimeException(errorMessage);
        }
    }

    private FullTextConfig getFullTextConfig(Field field, IndexType type) {
        if (type == IndexType.TEXT && field.isAnnotationPresent(FullText.class)) {
            return FullTextConfig.of(field.getAnnotation(FullText.class));
        }
        return null;
    }

    private HnswConfig getHnswConfig(Field field, IndexType type) {
        if (type == IndexType.HNSW && field.isAnnotationPresent(Hnsw.class)) {
            return HnswConfigs.of(field.getAnnotation(Hnsw.class));
        }
        return null;
    }

    private IvfConfig getIvfConfig(Field field, IndexType type) {
        if (type == IndexType.IVF && field.isAnnotationPresent(Ivf.class)) {
            return IvfConfigs.of(field.getAnnotation(Ivf.class));
        }
        return null;
    }

    private VecBfConfig getVecBfConfig(Field field, IndexType type) {
        if (type == IndexType.VEC_BF && field.isAnnotationPresent(VecBf.class)) {
            return VecBfConfigs.of(field.getAnnotation(VecBf.class));
        }
        return null;
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
                                       FullTextConfig textConfig, boolean isUuid, boolean isAppendable,
                                       boolean isNoColumn, HnswConfig hnswConfig, IvfConfig ivfConfig, VecBfConfig vecBfConfig) {
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
        index.setConfig(textConfig);
        index.setConfig(hnswConfig);
        index.setConfig(ivfConfig);
        index.setConfig(vecBfConfig);
        index.setUuid(isUuid);
        index.setAppendable(isAppendable);
        index.setNoColumn(isNoColumn);
        return index;
    }

    private FieldInfo getFieldInfo(Field field) {
        FieldInfo fieldInfo = new FieldInfo();
        FieldType fieldType;
        FieldConverter<?, ?> converter = FieldConverterRegistryFactory.INSTANCE.getFieldConverter(field);
        ResolvableType resolvableType;
        if (converter != null) {
            Pair<ResolvableType, ResolvableType> convertiblePair = converter.getConvertiblePair();
            resolvableType = convertiblePair.getSecond();
        } else {
            resolvableType = ConversionUtils.resolveFieldType(field);
        }
        fieldInfo.isArray = resolvableType.isCollectionLike();
        if (fieldInfo.isArray) {
            Class<?> componentType = getFieldType(field, resolvableType.getComponentType());
            fieldType = getFieldTypeByClass(componentType);
            fieldInfo.componentType = componentType;
            fieldInfo.isFloatVector = resolvableType.getType().isArray() && fieldType == FLOAT;
        } else {
            fieldType = getFieldTypeByClass(getFieldType(field, resolvableType.getType()));
        }

        if (fieldType == null) {
            throw new IllegalArgumentException("Unrecognized field type");
        }

        fieldInfo.fieldType = fieldType;
        return fieldInfo;
    }
    
    private Class<?> getFieldType(Field field, Class<?> type) {
        if (Enum.class.isAssignableFrom(type)) {
            Enumerated enumerated = field.getAnnotation(Enumerated.class);
            return enumerated != null && enumerated.value() == EnumType.STRING ? String.class : Integer.class;
        }
        return type;
    }

    private FieldType getFieldTypeByClass(Class<?> type) {
        return MAPPED_TYPES.getOrDefault(type, COMPOSITE);
    }

    private static class FieldInfo {
        private FieldType fieldType;
        private boolean isArray;
        private Class<?> componentType;
        private boolean isFloatVector;

        private boolean isInt() {
            return fieldType == INT || fieldType == INT64;
        }
    }

}
