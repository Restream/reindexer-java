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

import lombok.Getter;
import lombok.Setter;
import ru.rt.restream.reindexer.ReindexerIndex;

import java.util.List;

/**
 * Data-transfer object class, which is used to create namespace index.
 */
@Getter
@Setter
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

    private IndexConfig config;

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
        indexDefinition.setAppendable(index.isAppendable());
        indexDefinition.setConfig(index.getConfig());
        indexDefinition.setUuid(index.isUuid());
        return indexDefinition;
    }
}
