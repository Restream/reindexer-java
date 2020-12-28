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

import ru.rt.restream.reindexer.IndexType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static ru.rt.restream.reindexer.IndexType.DEFAULT;

@Target({ElementType.FIELD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Reindex {

    /**
     * Index name.
     *
     * @return an index name
     */
    String name();

    /**
     * Index type.
     *
     * @return an index type
     */
    IndexType type() default DEFAULT;

    /**
     * Reduce index size. For hash and tree it will save 8 bytes per unique key value. For - it will save 4-8 bytes per
     * each element. Useful for indexes with high selectivity, but for tree and hash indexes with low selectivity can
     * seriously decrease update performance. Also dense will slow down wide fullscan queries on - indexes, due to lack
     * of CPU cache optimization.
     *
     * @return true if the index size is reduced
     */
    boolean isDense() default false;

    /**
     * Row (document) contains a value of Sparse index only in case if it's set on purpose - there are no empty
     * (or default) records of this type of indexes in the row (document). It allows to save RAM but it will cost you
     * performance - it works a bit slower than regular indexes.
     *
     * @return true if the index contains a Sparse value
     */
    boolean isSparse() default false;

    boolean isPrimaryKey() default false;

    boolean isAppendable() default false;

    String collate() default "";

    /**
     * Used for composite indexes. Only for class-level annotation.
     *
     * @return a sub index names for a composite index
     */
    String[] subIndexes() default {};
}
