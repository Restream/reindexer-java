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

/**
 * Use the @Reindex annotation in code to create an index on a namespace. An index is a special structure defined for a
 * namespace. Queries are possible only on the indexed fields, marked with reindex annotation. An annotation can be
 * applied on an item class or on an item field. For the class it must define a set of subindexes.
 */
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

    /**
     * Index is a primary key. Item class must have at least 1 primary key.
     *
     * @return true, if index is a primary key
     */
    boolean isPrimaryKey() default false;

    /**
     * Additional fields can be appended to index.
     *
     * @return true, if index is appendable
     */
    boolean isAppendable() default false;

    /**
     * Index collate mode. Possible values - numeric, ascii, utf8, or custom. If custom collate mode used provide a
     * sequence of letters, which defines a sort order.
     *
     * @return the sequence of letters, which defines an index sort order
     */
    String collate() default "";

    /**
     * Used for composite indexes. Only for class-level annotation.
     *
     * @return a sub index names for a composite index
     */
    String[] subIndexes() default {};

    /**
     * Index is a UUID. Type of the index is only HASH.
     *
     * @return true, if index is UUID;
     */
    boolean isUuid() default false;
}
