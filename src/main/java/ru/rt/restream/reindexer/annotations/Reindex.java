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
     */
    String name();

    /**
     * Index type.
     */
    IndexType type() default DEFAULT;

    /**
     * Reduce index size. For hash and tree it will save 8 bytes per unique key value. For - it will save 4-8 bytes per
     * each element. Useful for indexes with high selectivity, but for tree and hash indexes with low selectivity can
     * seriously decrease update performance. Also dense will slow down wide fullscan queries on - indexes, due to lack
     * of CPU cache optimization.
     */
    boolean isDense() default false;

    /**
     * Row (document) contains a value of Sparse index only in case if it's set on purpose - there are no empty
     * (or default) records of this type of indexes in the row (document). It allows to save RAM but it will cost you
     * performance - it works a bit slower than regular indexes.
     */
    boolean isSparse() default false;

    boolean isPrimaryKey() default false;

    boolean isAppendable() default false;

    String collate() default "";

    /**
     * Used for composite indexes. Only for class-level annotation.
     */
    String[] fields() default {};
}
