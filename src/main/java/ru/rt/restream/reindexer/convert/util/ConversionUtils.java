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
package ru.rt.restream.reindexer.convert.util;

import org.apache.commons.lang3.reflect.TypeUtils;
import ru.rt.restream.reindexer.util.Pair;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * For internal use only, as this contract is likely to change.
 */
public final class ConversionUtils {

    private static final Map<Class<?>, Pair<ResolvableType, ResolvableType>> CONVERTIBLE_PAIR_CACHE = new ConcurrentHashMap<>();

    private static final Map<Pair<Class<?>, String>, ResolvableType> FIELD_TYPE_CACHE = new ConcurrentHashMap<>();

    /**
     * Resolves source and target types based on {@code [0]} and {@code [1]} parameters from {@code converterType} interface implementation.
     * If {@code converterType} source or target type is an array, the type is determined by {@code Class#getComponentType},
     * in case of a generic type, only {@code java.util.Collection} assignable types are considered, nested generic types
     * (i.e. {@code List<List<String>>}) are not supported, such types will not be recognized and IllegalArgumentException will be thrown.
     * @param converterClass the {@code converterType} interface implementation class to use
     * @param converterType the converter interface to use
     * @return the {@link Pair} of source and target {@link ResolvableType}s to use
     */
    public static Pair<ResolvableType, ResolvableType> resolveConvertiblePair(Class<?> converterClass, Class<?> converterType) {
        Objects.requireNonNull(converterClass, "converterClass must not be null");
        // https://bugs.openjdk.java.net/browse/JDK-8161372
        Pair<ResolvableType, ResolvableType> typePair = CONVERTIBLE_PAIR_CACHE.get(converterClass);
        if (typePair != null) {
            return typePair;
        }
        Objects.requireNonNull(converterType, "converterType must not be null");
        TypeVariable<? extends Class<?>>[] typeParameters = converterType.getTypeParameters();
        if (typeParameters.length < 2) {
            throw new IllegalArgumentException(
                    String.format("Converter type: %s must have 2 type arguments", converterType.getName()));
        }
        return CONVERTIBLE_PAIR_CACHE.computeIfAbsent(converterClass, k -> {
            Map<TypeVariable<?>, Type> typeArguments = TypeUtils.getTypeArguments(converterClass, converterType);
            Type sourceType = typeArguments.get(typeParameters[0]);
            ResolvableType resolvableSourceType = resolveType(sourceType);
            if (resolvableSourceType == null) {
                throw new IllegalArgumentException(
                        String.format("Cannot resolve source type: %s of converter type: %s for converter class: %s",
                                sourceType, converterType.getName(), converterClass.getName()));
            }
            Type targetType = typeArguments.get(typeParameters[1]);
            ResolvableType resolvableTargetType = resolveType(targetType);
            if (resolvableTargetType == null) {
                throw new IllegalArgumentException(
                        String.format("Cannot resolve target type: %s of converter type: %s for converter class: %s",
                                targetType, converterType.getName(), converterClass.getName()));
            }
            return new Pair<>(resolvableSourceType, resolvableTargetType);
        });
    }

    /**
     * Resolves a field type. If the field type is an array, the target type is determined
     * by {@code Class#getComponentType}, in case of a generic return type, only {@code java.util.Collection}
     * assignable types are considered, nested generic types (i.e. {@code List<List<String>>})
     * are not supported, such types will not be recognized and IllegalArgumentException will be thrown.
     * @param field the field to use
     * @return the {@link ResolvableType} to use
     */
    public static ResolvableType resolveFieldType(Field field) {
        Objects.requireNonNull(field, "field must not be null");
        // https://bugs.openjdk.java.net/browse/JDK-8161372
        Pair<Class<?>, String> key = new Pair<>(field.getDeclaringClass(), field.getName());
        ResolvableType resolvableType = FIELD_TYPE_CACHE.get(key);
        if (resolvableType != null) {
            return resolvableType;
        }
        return FIELD_TYPE_CACHE.computeIfAbsent(key, k -> {
            ResolvableType result = resolveType(field.getGenericType());
            if (result == null) {
                throw new IllegalArgumentException(String.format("Cannot resolve Field: %s.%s target type: %s",
                        field.getDeclaringClass().getName(), field.getName(), field.getGenericType()));
            }
            return result;
        });
    }

    private static ResolvableType resolveType(Type type) {
        if (TypeUtils.isAssignable(type, Collection.class)) {
            Type typeArgument = TypeUtils.getTypeArguments(type, Collection.class)
                    .get(Collection.class.getTypeParameters()[0]);
            if (typeArgument instanceof Class<?>) {
                Class<?> containerType = TypeUtils.getRawType(type, Collection.class);
                return new ResolvableType(containerType, (Class<?>) typeArgument, true);
            }
            return null;
        }
        if (type instanceof Class<?>) {
            Class<?> targetType = (Class<?>) type;
            return new ResolvableType(targetType, targetType.getComponentType(), targetType.isArray());
        }
        return null;
    }

    private ConversionUtils() {
        // utils
    }
}
