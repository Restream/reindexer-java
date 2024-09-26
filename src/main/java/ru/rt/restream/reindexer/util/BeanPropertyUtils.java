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
package ru.rt.restream.reindexer.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaConversionException;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ObjDoubleConsumer;
import java.util.function.ObjIntConsumer;
import java.util.function.ObjLongConsumer;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Utility methods for populating JavaBeans properties via reflection.
 */
public final class BeanPropertyUtils {

    private static final String SETTER_PREFIX = "set";

    private static final Pattern FIELD_SEPARATOR = Pattern.compile("\\.");

    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();

    private static final ClassValue<Map<String, BiConsumer>> SETTER_CACHE = new ClassValue<Map<String, BiConsumer>>() {
        @Override
        protected Map<String, BiConsumer> computeValue(Class<?> type) {
            return new ConcurrentHashMap<>();
        }
    };

    private static final ClassValue<Map<String, Function>> GETTER_CACHE = new ClassValue<Map<String, Function>>() {
        @Override
        protected Map<String, Function> computeValue(Class<?> type) {
            return new ConcurrentHashMap<>();
        }
    };

    private BeanPropertyUtils() {
        // utils
    }

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

    /**
     * Sets beanObject property value.
     *
     * @param beanObject   beanObject instance
     * @param propertyName property name
     * @param value        property value
     */
    public static void setProperty(Object beanObject, String propertyName, Object value) {
        getCachedSetter(beanObject.getClass(), propertyName).accept(beanObject, value);
    }

    private static BiConsumer getCachedSetter(Class<?> beanClass, String fieldName) {
        //https://bugs.openjdk.java.net/browse/JDK-8161372
        BiConsumer setter = SETTER_CACHE.get(beanClass).get(fieldName);
        if (setter != null) {
            return setter;
        }

        return SETTER_CACHE.get(beanClass).computeIfAbsent(fieldName, name -> createSetter(beanClass, name));
    }

    /**
     * Initializes a new instance of specified bean class.
     *
     * @param beanObject bean object to get property from
     * @param property   property name or path to property ("property.nestedProperty")
     * @return property value
     */
    public static Object getProperty(Object beanObject, String property) {
        return getCachedGetter(beanObject.getClass(), property).apply(beanObject);
    }

    private static Function getCachedGetter(Class<?> itemClass, String property) {
        final Function function = GETTER_CACHE.get(itemClass).get(property);
        if (function != null) {
            return function;
        }
        return GETTER_CACHE.get(itemClass).computeIfAbsent(property, name -> createGetter(itemClass, name));
    }

    private static BiConsumer createSetter(Class<?> itemClass, String property) {
        final boolean isBooleanField = property.length() > 2
                && property.startsWith("is")
                && FieldUtils.getField(itemClass, property, true).getType() == boolean.class;
        return Stream.of(itemClass.getMethods())
                .filter(BeanPropertyUtils::isSetterMethod)
                .filter(method -> method.getName().equals(SETTER_PREFIX + StringUtils.capitalize(property))
                                // lombok setter for Boolean field 'isValue' is 'setIsValue',
                                // but for boolean field 'isValue' setter is 'setValue'
                                || (isBooleanField
                                && method.getName().equals(SETTER_PREFIX + StringUtils.capitalize(property.substring(2)))
                        )
                )
                .findFirst()
                .map(method -> {
                    try {
                        return createSetter(LOOKUP.unreflect(method));
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                })
                .orElseThrow(() -> new IllegalStateException(
                        "Public setter is not found for the field: '" + property + "'"));
    }

    private static BiConsumer createSetter(MethodHandle setterMethodHandle) throws Throwable {
        Class<?> parameterType = setterMethodHandle.type().parameterType(1);
        if (parameterType == boolean.class) {
            ObjBooleanConsumer setter = (ObjBooleanConsumer) createCallSite(setterMethodHandle,
                    ObjBooleanConsumer.class, parameterType).getTarget().invokeExact();

            return (o, v) -> setter.accept(o, (boolean) v);
        } else if (parameterType == char.class) {
            ObjCharConsumer setter = (ObjCharConsumer) createCallSite(setterMethodHandle, ObjCharConsumer.class,
                    parameterType).getTarget().invokeExact();

            return (o, v) -> setter.accept(o, (char) v);
        } else if (parameterType == byte.class) {
            ObjByteConsumer setter = (ObjByteConsumer) createCallSite(setterMethodHandle, ObjByteConsumer.class,
                    parameterType).getTarget().invokeExact();

            return (o, v) -> setter.accept(o, (byte) v);
        } else if (parameterType == short.class) {
            ObjShortConsumer setter = (ObjShortConsumer) createCallSite(setterMethodHandle, ObjShortConsumer.class,
                    parameterType).getTarget().invokeExact();

            return (o, v) -> setter.accept(o, (short) v);
        } else if (parameterType == int.class) {
            ObjIntConsumer setter = (ObjIntConsumer) createCallSite(setterMethodHandle, ObjIntConsumer.class,
                    parameterType).getTarget().invokeExact();

            return (o, v) -> setter.accept(o, (int) v);
        } else if (parameterType == long.class) {
            ObjLongConsumer setter = (ObjLongConsumer) createCallSite(setterMethodHandle, ObjLongConsumer.class,
                    parameterType).getTarget().invokeExact();

            return (o, v) -> setter.accept(o, (long) v);
        } else if (parameterType == float.class) {
            ObjFloatConsumer setter = (ObjFloatConsumer) createCallSite(setterMethodHandle, ObjFloatConsumer.class,
                    parameterType).getTarget().invokeExact();

            return (o, v) -> setter.accept(o, (float) v);
        } else if (parameterType == double.class) {
            ObjDoubleConsumer setter = (ObjDoubleConsumer) createCallSite(setterMethodHandle, ObjDoubleConsumer.class,
                    parameterType).getTarget().invokeExact();

            return (o, v) -> setter.accept(o, (double) v);
        } else {
            return (BiConsumer) createCallSite(setterMethodHandle, BiConsumer.class, Object.class).getTarget()
                    .invokeExact();
        }
    }

    private static Pair<? extends Class, Function> createGetter(String property, Class<?> itemClass) {
        return Stream.of(itemClass.getDeclaredMethods())
                .filter(BeanPropertyUtils::isGetterMethod)
                .filter(getterMethod -> isGetterMethodForFieldName(getterMethod, property))
                .map(BeanPropertyUtils::createPairWithReturnTypeAndGetter)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "Public getter is not found for the field: '" + property + "' of " + itemClass.getName()));
    }

    private static Function createGetter(Class<?> itemClass, String property) {
        List<Function> functions = new ArrayList<>();
        Stream.of(FIELD_SEPARATOR.split(property))
                .reduce(itemClass, (nestedJavaBeanClass, nestedFieldName) -> {
                    Pair<? extends Class, Function> getFunction = createGetter(nestedFieldName, nestedJavaBeanClass);
                    functions.add(getFunction.getSecond());
                    return getFunction.getFirst();
                }, (previousClass, nextClass) -> nextClass);
        return functions.stream()
                .reduce(Function::andThen)
                .orElseThrow(IllegalStateException::new);
    }

    private static boolean isGetterMethodForFieldName(Method getterMethod, String fieldName) {
        return getterMethod.getName().equals(fieldName)
                || getterMethod.getName().equals("get" + StringUtils.capitalize(fieldName))
                || getterMethod.getName().equals("is" + StringUtils.capitalize(fieldName));
    }

    private static Pair<? extends Class, Function> createPairWithReturnTypeAndGetter(Method getterMethod) {
        try {
            return new Pair<>(getterMethod.getReturnType(),
                    (Function) createCallSite(LOOKUP.unreflect(getterMethod)).getTarget().invokeExact());
        } catch (Throwable e) {
            throw new IllegalArgumentException(
                    "Lambda creation failed for getterMethod (" + getterMethod.getName() + ").", e);
        }
    }

    public static PropertyDescriptor[] getPropertyDescriptors(Class<?> itemClass) {
        try {
            return Introspector.getBeanInfo(itemClass, Object.class).getPropertyDescriptors();
        } catch (IntrospectionException e) {
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    private interface ObjBooleanConsumer {
        void accept(Object o, boolean value);
    }

    @FunctionalInterface
    private interface ObjCharConsumer {
        void accept(Object o, char value);
    }

    @FunctionalInterface
    private interface ObjByteConsumer {
        void accept(Object o, byte value);
    }

    @FunctionalInterface
    private interface ObjShortConsumer {
        void accept(Object o, short value);
    }

    @FunctionalInterface
    private interface ObjFloatConsumer {
        void accept(Object o, float value);
    }

    private static boolean isSetterMethod(Method method) {
        return method.getParameterCount() == 1
                && !Modifier.isStatic(method.getModifiers()) && method.getName().startsWith(SETTER_PREFIX);
    }

    private static boolean isGetterMethod(Method method) {
        return method.getParameterCount() == 0
                && !Modifier.isStatic(method.getModifiers())
                && (method.getName().startsWith("get") || method.getName().startsWith("is"))
                && !method.getName().endsWith("Class");
    }

    private static boolean isBeanField(Field field) {
        return !field.isSynthetic() && !Modifier.isStatic(field.getModifiers());
    }

    private static CallSite createCallSite(MethodHandle setterMethodHandle,
                                           Class<?> interfaceType,
                                           Class<?> parameterType) throws LambdaConversionException {
        return LambdaMetafactory.metafactory(LOOKUP, "accept",
                MethodType.methodType(interfaceType),
                MethodType.methodType(void.class, Object.class, parameterType),
                setterMethodHandle, setterMethodHandle.type());
    }

    private static CallSite createCallSite(MethodHandle getterMethodHandle) throws LambdaConversionException {
        return LambdaMetafactory.metafactory(LOOKUP, "apply",
                MethodType.methodType(Function.class),
                MethodType.methodType(Object.class, Object.class),
                getterMethodHandle, getterMethodHandle.type());
    }

}
