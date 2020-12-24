/**
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

    private String collateMode;

    private String sortOrder;

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
        return indexDefinition;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getJsonPaths() {
        return jsonPaths;
    }

    public void setJsonPaths(List<String> jsonPaths) {
        this.jsonPaths = jsonPaths;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public boolean isPk() {
        return isPk;
    }

    public void setPk(boolean pk) {
        isPk = pk;
    }

    public boolean isArray() {
        return isArray;
    }

    public void setArray(boolean array) {
        isArray = array;
    }

    public boolean isDense() {
        return isDense;
    }

    public void setDense(boolean dense) {
        isDense = dense;
    }

    public boolean isSparse() {
        return isSparse;
    }

    public void setSparse(boolean sparse) {
        isSparse = sparse;
    }

    public boolean isLinear() {
        return isLinear;
    }

    public void setLinear(boolean linear) {
        isLinear = linear;
    }

    public boolean isAppendable() {
        return isAppendable;
    }

    public void setAppendable(boolean appendable) {
        isAppendable = appendable;
    }

    public String getCollateMode() {
        return collateMode;
    }

    public void setCollateMode(String collateMode) {
        this.collateMode = collateMode;
    }

    public String getSortOrder() {
        return sortOrder;
    }

    public void setSortOrder(String sortOrder) {
        this.sortOrder = sortOrder;
    }
}
