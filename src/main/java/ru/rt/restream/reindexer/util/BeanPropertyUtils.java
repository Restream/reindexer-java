package ru.rt.restream.reindexer.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class BeanPropertyUtils {

    /**
     * Get declared and inherited fields of a given object.
     *
     * @param type to introspect
     * @return declared and inherited fields
     */
    public static List<Field> getInheritedFields(Class<?> type) {
        List<Field> fields = new ArrayList<>();

        Class<?> stopClass = type.isEnum() ? Enum.class : Object.class;
        Class<?> beanClass = type;
        while (beanClass != null && beanClass != stopClass) {
            Stream.of(beanClass.getDeclaredFields())
                    .filter(BeanPropertyUtils::isBeanField)
                    .forEach(fields::add);
            beanClass = beanClass.getSuperclass();
        }

        return fields;
    }

    private static boolean isBeanField(Field field) {
        return !field.isSynthetic() && !Modifier.isStatic(field.getModifiers());
    }

}
