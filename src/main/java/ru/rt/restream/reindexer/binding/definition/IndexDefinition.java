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
package ru.rt.restream.reindexer.binding.definition;

import ru.rt.restream.reindexer.ReindexerIndex;
import ru.rt.restream.reindexer.fulltext.FullTextConfig;

import java.util.List;

/**
 * Data-transfer object class, which is used to create namespace index.
 */
public class IndexDefinition {

    private String name;

    private List<String> jsonPaths;

    private String indexType;

    private String fieldType;

    private boolean isPk;

    private boolean isArray;

    private boolean isDense;

    private boolean isSparse;

    private boolean isLinear;

    private boolean isAppendable;

    private boolean isUuid;

    private String collateMode;

    private String sortOrder;

    private FullTextConfig config;

    /**
     * Construct a new IndexDefinition from a {@link ReindexerIndex} object.
     *
     * @param index the object by which the new index definition is constructed
     * @return a new IndexDefinition
     */
    public static IndexDefinition fromIndex(ReindexerIndex index) {
        IndexDefinition indexDefinition = new IndexDefinition();
        indexDefinition.setName(index.getName());
        indexDefinition.setCollateMode(index.getCollateMode().getName());
        indexDefinition.setSortOrder(index.getSortOrder());
        indexDefinition.setDense(index.isDense());
        indexDefinition.setFieldType(index.getFieldType().getName());
        indexDefinition.setIndexType(index.getIndexType().getName());
        indexDefinition.setArray(index.isArray());
        indexDefinition.setPk(index.isPk());
        indexDefinition.setJsonPaths(index.getJsonPaths());
        indexDefinition.setSparse(index.isSparse());
        indexDefinition.setLinear(false);
        indexDefinition.setAppendable(false);
        indexDefinition.setConfig(index.getFullTextConfig());
        indexDefinition.setUuid(index.isUuid());
        return indexDefinition;
    }

    /**
     * Get the current index name.
     *
     * @return the current index name
     */
    public String getName() {
        return name;
    }

    /**
     * Set the current index name.
     *
     * @param name the index name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get the current index json paths.
     *
     * @return the current index json paths
     */
    public List<String> getJsonPaths() {
        return jsonPaths;
    }

    /**
     * Set the current index json paths.
     *
     * @param jsonPaths the index json paths
     */
    public void setJsonPaths(List<String> jsonPaths) {
        this.jsonPaths = jsonPaths;
    }

    /**
     * Get the current index type name.
     *
     * @return the current index type name
     */
    public String getIndexType() {
        return indexType;
    }

    /**
     * Set the current index type.
     *
     * @param indexType the index type name. {@link ru.rt.restream.reindexer.IndexType}
     */
    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    /**
     * Get the current index field type.
     *
     * @return the current index field type name
     */
    public String getFieldType() {
        return fieldType;
    }

    /**
     * Set the current index field type.
     *
     * @param fieldType the index field type {@link ru.rt.restream.reindexer.FieldType}
     */
    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    /**
     * Get the indication, that the current index is a primary key.
     *
     * @return true, if the current index is a primary key
     */
    public boolean isPk() {
        return isPk;
    }

    /**
     * Set the indication, that the current index is a primary key.
     *
     * @param pk true, if the current index is a primary key
     */
    public void setPk(boolean pk) {
        isPk = pk;
    }

    /**
     * Get the indication, that the current index is array.
     *
     * @return true, if the current index is array
     */
    public boolean isArray() {
        return isArray;
    }

    /**
     * Set the indication, that the current index is array.
     *
     * @param array true, if the current index is array
     */
    public void setArray(boolean array) {
        isArray = array;
    }

    /**
     * Get the indication, that the current index is dense.
     *
     * @return true, is current index is dense
     */
    public boolean isDense() {
        return isDense;
    }

    /**
     * Set the indication, that the current index is dense.
     *
     * @param dense true, if the current index is dense
     */
    public void setDense(boolean dense) {
        isDense = dense;
    }

    /**
     * Get the indication, that the current index is sparse.
     *
     * @return true, if the current index is sparse
     */
    public boolean isSparse() {
        return isSparse;
    }

    /**
     * Set the indication, that the current index is sparse.
     *
     * @param sparse true, if the current index is sparse
     */
    public void setSparse(boolean sparse) {
        isSparse = sparse;
    }

    /**
     * Get the indication, that the current index is linear.
     *
     * @return true, if the current index is linear
     */
    public boolean isLinear() {
        return isLinear;
    }

    /**
     * Set the indication, that the current index is linear.
     *
     * @param linear true, if the current index is linear
     */
    public void setLinear(boolean linear) {
        isLinear = linear;
    }

    /**
     * Get the indication, that the current index is appendable.
     *
     * @return true, is the current index is appendable
     */
    public boolean isAppendable() {
        return isAppendable;
    }

    /**
     * Set the indication, that the current index is appendable.
     *
     * @param appendable true, if the current index is appendable
     */
    public void setAppendable(boolean appendable) {
        isAppendable = appendable;
    }

    /**
     * Get the indication, that the current index is appendable.
     *
     * @return true, is the current index is appendable
     */
    public boolean isUuid() {
        return isUuid;
    }

    /**
     * Set the indication, that the current index is for UUID.
     *
     * @param uuid true, if the current index is for UUID
     */
    public void setUuid(boolean uuid) {
        isUuid = uuid;
    }

    /**
     * Get the current index collate mode.
     *
     * @return the current index collate mode name. {@link ru.rt.restream.reindexer.CollateMode}
     */
    public String getCollateMode() {
        return collateMode;
    }

    /**
     * Set the current index collate mode.
     *
     * @param collateMode the current index collate mode name. {@link ru.rt.restream.reindexer.CollateMode}
     */
    public void setCollateMode(String collateMode) {
        this.collateMode = collateMode;
    }

    /**
     * Get the current index sort order.
     *
     * @return the current index sort order
     */
    public String getSortOrder() {
        return sortOrder;
    }

    /**
     * Set the current index sort order.
     *
     * @param sortOrder the current index sort order
     */
    public void setSortOrder(String sortOrder) {
        this.sortOrder = sortOrder;
    }

    /**
     * Get full text search config for current index.
     * It has meaning only if the index is text index.
     *
     * @return full text search config.
     */
    public FullTextConfig getConfig() {
        return config;
    }

    /**
     * Set full text search config for current index.
     * Do this only if the index is text index.
     *
     * @param config full text search configuration of text index
     */
    public void setConfig(FullTextConfig config) {
        this.config = config;
    }
}
