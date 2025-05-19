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

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * This class contains utility methods to work with Java collections.
 */
public final class CollectionUtils {

    /**
     * Creates the most appropriate collection for the given {@code collectionType}.
     * For {@code EnumSet} collection type the {@code elementType} must not be {@literal null}
     * and must be an {@code Enum} type matching to {@code E} type.
     * @param collectionType the target collection type to create
     * @param elementType the element collection type
     * @param capacity the collection initial capacity
     * @param <E> the collection type argument
     * @return the {@link Collection} to use   
     */
    @SuppressWarnings("unchecked")
    public static <E> Collection<E> createCollection(Class<?> collectionType, Class<?> elementType, int capacity) {
        Objects.requireNonNull(collectionType, "Collection type must not be null");
        if (collectionType == Collection.class ||
            collectionType == List.class ||
            collectionType == ArrayList.class) {
            return new ArrayList<>(capacity);
        }
        if (collectionType == Set.class ||
            collectionType == LinkedHashSet.class ||
            // Java 21 collection types.
            "java.util.SequencedSet".equals(collectionType.getName()) ||
            "java.util.SequencedCollection".equals(collectionType.getName())) {
            return new LinkedHashSet<>(capacity);
        }
        if (collectionType == HashSet.class) {
            return new HashSet<>(capacity);
        }
        if (collectionType == LinkedList.class) {
            return new LinkedList<>();
        }
        if (collectionType == TreeSet.class ||
            collectionType == NavigableSet.class ||
            collectionType == SortedSet.class) {
            return new TreeSet<>();
        }
        if (EnumSet.class.isAssignableFrom(collectionType)) {
            return EnumSet.noneOf(asEnumType(elementType));
        }
        if (collectionType.isInterface() || !Collection.class.isAssignableFrom(collectionType)) {
            throw new IllegalArgumentException("Unsupported Collection type: " + collectionType.getName());
        }
        try {
            return (Collection<E>) collectionType.getDeclaredConstructor().newInstance();
        } catch (Throwable e) {
            throw new IllegalArgumentException(
                    "Could not instantiate Collection type: " + collectionType.getName(), e);
        }
    }

    @SuppressWarnings("rawtypes")
    private static Class<? extends Enum> asEnumType(Class<?> enumType) {
        Objects.requireNonNull(enumType, "Enum type must not be null");
        if (!Enum.class.isAssignableFrom(enumType)) {
            throw new IllegalArgumentException("Supplied type is not an enum: " + enumType.getName());
        }
        return enumType.asSubclass(Enum.class);
    }

    private CollectionUtils() {
        // utils
    }
}
