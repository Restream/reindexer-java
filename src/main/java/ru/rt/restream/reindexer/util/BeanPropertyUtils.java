/**
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
