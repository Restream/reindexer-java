package ru.rt.restream.reindexer;

import java.util.List;

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

    public IndexType getIndexType() {
        return indexType;
    }

    public void setIndexType(IndexType indexType) {
        this.indexType = indexType;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    public void setFieldType(FieldType fieldType) {
        this.fieldType = fieldType;
    }

    public CollateMode getCollateMode() {
        return collateMode;
    }

    public void setCollateMode(CollateMode collateMode) {
        this.collateMode = collateMode;
    }

    public String getSortOrder() {
        return sortOrder;
    }

    public void setSortOrder(String sortOrder) {
        this.sortOrder = sortOrder;
    }

    public String getPrecept() {
        return precept;
    }

    public void setPrecept(String precept) {
        this.precept = precept;
    }

    public boolean isArray() {
        return isArray;
    }

    public void setArray(boolean array) {
        isArray = array;
    }

    public boolean isPk() {
        return isPk;
    }

    public void setPk(boolean pk) {
        isPk = pk;
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
}
