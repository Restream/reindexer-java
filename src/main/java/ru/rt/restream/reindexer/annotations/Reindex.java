package ru.rt.restream.reindexer.annotations;

import ru.rt.restream.reindexer.Index;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies the namespace index for an item field.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Reindexes.class)
public @interface Reindex {

    /**
     * Index name. If not specified - field name is used.
     */
    String name() default "";

    /**
     * Index type.
     */
    String type() default "";

    /**
     * Additional index options. {@link Index.Option}
     */
    Index.Option[] options() default {};

    /**
     * Specifies namespace for which index belongs. If not specified index created in all item namespaces.
     */
    String nameSpace() default "";

}
