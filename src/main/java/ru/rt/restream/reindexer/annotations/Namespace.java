package ru.rt.restream.reindexer.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

//TODO:
@Target(ElementType.TYPE)
public @interface Namespace {

    String name();

    boolean enableStorage() default true;

    boolean dropOnIndexConflict() default false;

    boolean dropOnFileFormatError() default false;

    boolean disableObjCache() default false;

    long objCacheItemsCount() default 256000L;

}
