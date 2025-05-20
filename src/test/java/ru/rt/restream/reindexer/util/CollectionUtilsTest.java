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

import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link CollectionUtils}.
 */
class CollectionUtilsTest {
    
    @Test
    void createCollectionWhenCollectionThenArrayList() {
        Collection<Object> collection = CollectionUtils.createCollection(Collection.class, null, 0);
        assertThat(collection, notNullValue());
        assertThat(collection, instanceOf(ArrayList.class));
    }

    @Test
    void createCollectionWhenListThenArrayList() {
        Collection<Object> collection = CollectionUtils.createCollection(List.class, null, 0);
        assertThat(collection, notNullValue());
        assertThat(collection, instanceOf(ArrayList.class));
    }

    @Test
    void createCollectionWhenArrayListThenArrayList() {
        Collection<Object> collection = CollectionUtils.createCollection(ArrayList.class, null, 0);
        assertThat(collection, notNullValue());
        assertThat(collection, instanceOf(ArrayList.class));
    }

    @Test
    void createCollectionWhenSetThenLinkedHashSet() {
        Collection<Object> collection = CollectionUtils.createCollection(Set.class, null, 0);
        assertThat(collection, notNullValue());
        assertThat(collection, instanceOf(LinkedHashSet.class));
    }

    @Test
    void createCollectionWhenLinkedHashSetThenLinkedHashSet() {
        Collection<Object> collection = CollectionUtils.createCollection(LinkedHashSet.class, null, 0);
        assertThat(collection, notNullValue());
        assertThat(collection, instanceOf(LinkedHashSet.class));
    }

    @Test
    void createCollectionWhenHashSetThenHashSet() {
        Collection<Object> collection = CollectionUtils.createCollection(HashSet.class, null, 0);
        assertThat(collection, notNullValue());
        assertThat(collection, instanceOf(HashSet.class));
    }

    @Test
    void createCollectionWhenLinkedListThenLinkedList() {
        Collection<Object> collection = CollectionUtils.createCollection(LinkedList.class, null, 0);
        assertThat(collection, notNullValue());
        assertThat(collection, instanceOf(LinkedList.class));
    }

    @Test
    void createCollectionWhenTreeSetThenTreeSet() {
        Collection<Object> collection = CollectionUtils.createCollection(TreeSet.class, null, 0);
        assertThat(collection, notNullValue());
        assertThat(collection, instanceOf(TreeSet.class));
    }

    @Test
    void createCollectionWhenNavigableSetThenNavigableSet() {
        Collection<Object> collection = CollectionUtils.createCollection(NavigableSet.class, null, 0);
        assertThat(collection, notNullValue());
        assertThat(collection, instanceOf(TreeSet.class));
    }

    @Test
    void createCollectionWhenSortedSetThenSortedSet() {
        Collection<Object> collection = CollectionUtils.createCollection(SortedSet.class, null, 0);
        assertThat(collection, notNullValue());
        assertThat(collection, instanceOf(TreeSet.class));
    }

    @Test
    void createCollectionWhenEnumSetThenEnumSet() {
        Collection<Object> collection = CollectionUtils.createCollection(EnumSet.class, TestEnum.class, 0);
        assertThat(collection, notNullValue());
        assertThat(collection, instanceOf(EnumSet.class));
    }

    @Test
    void createCollectionWhenArrayDequeThenArrayDeque() {
        Collection<Object> collection = CollectionUtils.createCollection(ArrayDeque.class, null, 0);
        assertThat(collection, notNullValue());
        assertThat(collection, instanceOf(ArrayDeque.class));
    }

    @Test
    void createCollectionWhenEnumSetElementTypeNullThenException() {
        NullPointerException exception = assertThrows(NullPointerException.class,
                () -> CollectionUtils.createCollection(EnumSet.class, null, 0));
        assertThat(exception.getMessage(), is("Enum type must not be null"));
    }

    @Test
    void createCollectionWhenEnumSetElementTypeNotEnumThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> CollectionUtils.createCollection(EnumSet.class, Object.class, 0));
        assertThat(exception.getMessage(), is("Supplied type is not an enum: java.lang.Object"));
    }

    @Test
    void createCollectionWhenQueueThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> CollectionUtils.createCollection(Queue.class, null, 0));
        assertThat(exception.getMessage(), is("Unsupported Collection type: java.util.Queue"));
    }

    @Test
    void createCollectionWhenHashMapThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> CollectionUtils.createCollection(HashMap.class, null, 0));
        assertThat(exception.getMessage(), is("Unsupported Collection type: java.util.HashMap"));
    }

    enum TestEnum {
        TEST_CONSTANT
    }
}
