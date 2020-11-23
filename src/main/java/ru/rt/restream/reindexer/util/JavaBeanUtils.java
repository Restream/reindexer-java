package ru.rt.restream.reindexer.util;

import org.apache.commons.lang3.StringUtils;

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaConversionException;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
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

public class JavaBeanUtils {
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

    private JavaBeanUtils() {
        // utils
    }

    /**
     * Устанавливает значение свойству объекта.
     *
     * @param javaBean   объект
     * @param fieldName  имя свойства
     * @param fieldValue значение свойства
     */
    public static void setProperty(Object javaBean, String fieldName, Object fieldValue) {
        getCachedSetter(javaBean.getClass(), fieldName).accept(javaBean, fieldValue);
    }

    private static BiConsumer getCachedSetter(Class<?> beanClass, String fieldName) {
        // Фикс для Java 8, computeIfAbsent получает блокировку даже когда для ключа есть значение.
        // Подробнее: https://bugs.openjdk.java.net/browse/JDK-8161372
        BiConsumer setter = SETTER_CACHE.get(beanClass).get(fieldName);
        if (setter != null) {
            return setter;
        }

        return SETTER_CACHE.get(beanClass).computeIfAbsent(fieldName, name -> createSetter(beanClass, name));
    }

    private static BiConsumer createSetter(Class<?> beanClass, String fieldName) {
        return Stream.of(beanClass.getMethods())
                .filter(JavaBeanUtils::isSetterMethod)
                .filter(method -> method.getName().equals(SETTER_PREFIX + StringUtils.capitalize(fieldName)))
                .map(method -> {
                    try {
                        return createSetter(LOOKUP.unreflect(method));
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                })
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "Public setter is not found for the field: '" + fieldName + "'"));
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

    /**
     * Возвращает значение свойства объекта.
     *
     * @param <T>       тип возвращаемого значения
     * @param javaBean  объект
     * @param fieldName имя свойства (или путь в формате "fieldName.nestedFieldName")
     * @return значение свойства объекта
     */
    public static <T> T getProperty(Object javaBean, String fieldName) {
        return (T) getCachedGetter(javaBean.getClass(), fieldName).apply(javaBean);
    }

    private static Function getCachedGetter(Class<?> javaBeanClass, String fieldName) {
        final Function function = GETTER_CACHE.get(javaBeanClass).get(fieldName);
        if (function != null) {
            return function;
        }
        return createAndCacheGetter(javaBeanClass, fieldName);
    }

    private static Function createAndCacheGetter(Class<?> javaBeanClass, String path) {
        return cacheAndGetGetter(path, javaBeanClass,
                createGetters(javaBeanClass, path)
                        .stream()
                        .reduce(Function::andThen)
                        .orElseThrow(IllegalStateException::new)
        );
    }

    private static Function cacheAndGetGetter(String path, Class<?> javaBeanClass, Function functionToBeCached) {
        Function cachedFunction = GETTER_CACHE.get(javaBeanClass).putIfAbsent(path, functionToBeCached);
        return cachedFunction != null ? cachedFunction : functionToBeCached;
    }

    private static List<Function> createGetters(Class<?> javaBeanClass, String path) {
        List<Function> functions = new ArrayList<>();
        Stream.of(FIELD_SEPARATOR.split(path))
                .reduce(javaBeanClass, (nestedJavaBeanClass, fieldName) -> {
                    Pair<? extends Class, Function> getFunction = createGetter(fieldName, nestedJavaBeanClass);
                    functions.add(getFunction.getSecond());
                    return getFunction.getFirst();
                }, (previousClass, nextClass) -> nextClass);
        return functions;
    }

    private static Pair<? extends Class, Function> createGetter(String fieldName, Class<?> javaBeanClass) {
        return Stream.of(javaBeanClass.getDeclaredMethods())
                .filter(JavaBeanUtils::isGetterMethod)
                .filter(method -> StringUtils.endsWithIgnoreCase(method.getName(), fieldName))
                .map(JavaBeanUtils::createPairWithReturnTypeAndGetter)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "Public getter is not found for the field: '" + fieldName + "'"));
    }

    private static Pair<? extends Class, Function> createPairWithReturnTypeAndGetter(Method getterMethod) {
        try {
            return Pair.<Class<?>, Function>builder()
                    .first(getterMethod.getReturnType())
                    .second((Function) createCallSite(LOOKUP.unreflect(getterMethod)).getTarget().invokeExact())
                    .build();
        } catch (Throwable e) {
            throw new IllegalArgumentException(
                    "Lambda creation failed for getterMethod (" + getterMethod.getName() + ").", e);
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
