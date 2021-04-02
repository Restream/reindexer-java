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
package ru.rt.restream.reindexer;

import java.util.List;

/**
 * Contains the reindexer index configuration.
 */
public class ReindexerIndex {

    private String name;

    private List<String> jsonPaths;

    private IndexType indexType;

    private FieldType fieldType;

    private CollateMode collateMode;

    private String sortOrder;

    private String precept;

    private boolean isArray;

    private boolean isPk;

    private boolean isDense;

    private boolean isSparse;

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
     * Get the current index type. {@link IndexType}
     *
     * @return the current index type
     */
    public IndexType getIndexType() {
        return indexType;
    }

    /**
     * Set the current index type. {@link IndexType}
     *
     * @param indexType the index type
     */
    public void setIndexType(IndexType indexType) {
        this.indexType = indexType;
    }

    /**
     * Get the current index field type. {@link FieldType}
     *
     * @return the current index field type
     */
    public FieldType getFieldType() {
        return fieldType;
    }

    /**
     * Set the current index field type. {@link FieldType}
     *
     * @param fieldType the index field type
     */
    public void setFieldType(FieldType fieldType) {
        this.fieldType = fieldType;
    }

    /**
     * Get the current index collate mode. {@link CollateMode}
     *
     * @return the current index collate mode
     */
    public CollateMode getCollateMode() {
        return collateMode;
    }

    /**
     * Set the current index collate mode. {@link CollateMode}
     *
     * @param collateMode the index collate mode
     */
    public void setCollateMode(CollateMode collateMode) {
        this.collateMode = collateMode;
    }

    /**
     * Get the current index sort order.
     *
     * @return the current index sort order string
     */
    public String getSortOrder() {
        return sortOrder;
    }

    /**
     * Set the current index sort order.
     *
     * @param sortOrder the sequence of letters, which defines the index sort order
     */
    public void setSortOrder(String sortOrder) {
        this.sortOrder = sortOrder;
    }

    /**
     * Get the current index precept. Precept is a special reindexer embedded function, such as serial(), now().
     * {@link ru.rt.restream.reindexer.annotations.Serial}
     *
     * @return the current index precept
     */
    public String getPrecept() {
        return precept;
    }

    /**
     * Set the current index precepts. Precept is a special reindexer embedded function, such as serial(), now().
     * {@link ru.rt.restream.reindexer.annotations.Serial}
     *
     * @param precept the index precept
     */
    public void setPrecept(String precept) {
        this.precept = precept;
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
     * Get the indication, that the current index is dense.
     *
     * @return true, if the current index is dense
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
}
