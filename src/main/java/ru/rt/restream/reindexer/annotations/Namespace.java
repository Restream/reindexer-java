package ru.rt.restream.reindexer.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies the reindexer namespace for an item class. If annotation is not present - default namespace options are
 * used.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Namespaces.class)
public @interface Namespace {

    /**
     * Namespace name.
     */
    String name() default "";

    /**
     * Use external storage for items. Reindexer's core uses LevelDB as the storage backend.
     */
    boolean enableStorage() default true;

    /**
     * Create storage directory for namespace if it doesn't exists.
     */
    boolean createStorageIfMissing() default true;

    /**
     * Drop existing storage on file format error.
     */
    boolean dropStorageOnFileFormatError() default false;

    /**
     * Drop existing namespace index if it conflicts with newly created.
     */
    boolean dropOnIndexConflict() default ru.rt.restream.reindexer.Namespace.DEFAULT_DROP_ON_INDEX_CONFLICT;

    /**
     * Disables object caching.
     */
    boolean disableObjCache() default ru.rt.restream.reindexer.Namespace.DEFAULT_DISABLE_OBJ_CACHE;

    /**
     * Default cache items count.
     */
    long objCacheItemsCount() default ru.rt.restream.reindexer.Namespace.DEFAULT_OBJ_CACHE_ITEMS_COUNT;

}
