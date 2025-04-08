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

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import ru.rt.restream.reindexer.binding.definition.IndexConfig;
import ru.rt.restream.reindexer.fulltext.FullTextConfig;
import ru.rt.restream.reindexer.vector.HnswConfig;
import ru.rt.restream.reindexer.vector.IvfConfig;
import ru.rt.restream.reindexer.vector.VecBfConfig;

import java.util.List;

/**
 * Contains the reindexer index configuration.
 *
 * `equals()` is used to compare index configuration without the jsonPaths field.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@EqualsAndHashCode
public class ReindexerIndex {

    private String name;

    @EqualsAndHashCode.Exclude
    private List<String> jsonPaths;

    private IndexType indexType;

    private FieldType fieldType;

    private CollateMode collateMode;

    private String sortOrder;

    /**
     * Full text search config for current index.
     * Type of index must be TEXT.
     */
    @Setter(AccessLevel.NONE)
    private FullTextConfig fullTextConfig;

    /**
     * Float vector search config for current index.
     * Type of index must be HNSW.
     */
    @Setter(AccessLevel.NONE)
    private HnswConfig hnswConfig;

    /**
     * Float vector search config for current index.
     * Type of index must be IVF.
     */
    @Setter(AccessLevel.NONE)
    private IvfConfig ivfConfig;

    /**
     * Float vector search config for current index.
     * Type of index must be VEC_BF.
     */
    @Setter(AccessLevel.NONE)
    private VecBfConfig vecBfConfig;

    /**
     * Precept is a special reindexer embedded function, such as serial(), now().
     * {@link ru.rt.restream.reindexer.annotations.Serial}
     */
    private String precept;

    private boolean isArray;

    /**
     * Indication, that the current index is a primary key.
     */
    private boolean isPk;

    private boolean isDense;

    private boolean isNoColumn;

    private boolean isSparse;

    private boolean isUuid;

    private boolean isAppendable;

    /**
     * Set full text search config for current index, if the index is text index.
     *
     * @param fullTextConfig full text search configuration of text index
     * @throws IllegalArgumentException if type of index is not TEXT and fullText config is not null
     */
    public void setConfig(FullTextConfig fullTextConfig) {
        if (indexType != IndexType.TEXT && fullTextConfig != null) {
            throw new IllegalArgumentException("Type of index must be TEXT for full text search config.");
        }
        this.fullTextConfig = fullTextConfig;
    }

    public void setConfig(HnswConfig hnswConfig) {
        if (indexType != IndexType.HNSW && hnswConfig != null) {
            throw new IllegalArgumentException("Type of index must be HNSW for float vector search config.");
        }
        this.hnswConfig = hnswConfig;
    }

    public void setConfig(IvfConfig ivfConfig) {
        if (indexType != IndexType.IVF && ivfConfig != null) {
            throw new IllegalArgumentException("Type of index must be IVF for float vector search config.");
        }
        this.ivfConfig = ivfConfig;
    }

    public void setConfig(VecBfConfig vecBfConfig) {
        if (indexType != IndexType.VEC_BF && vecBfConfig != null) {
            throw new IllegalArgumentException("Type of index must be VEC_BF for float vector search config.");
        }
        this.vecBfConfig = vecBfConfig;
    }

    public IndexConfig getConfig() {
        switch (indexType) {
            case TEXT:
                return fullTextConfig;
            case HNSW:
                return hnswConfig;
            case IVF:
                return ivfConfig;
            case VEC_BF:
                return vecBfConfig;
            default:
                return null;
        }
    }
}
