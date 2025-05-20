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

import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.Test;
import ru.rt.restream.reindexer.convert.FieldConverter;
import ru.rt.restream.reindexer.util.Pair;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link ConversionUtils}.
 */
public class ConversionUtilsTest {

    @Test
    void resolveConvertiblePairWhenClassImplementsFieldConverterThenResolves() {
        Pair<ResolvableType, ResolvableType> typePair = ConversionUtils
                .resolveConvertiblePair(ClassImplementsFieldConverter.class, FieldConverter.class);
        ResolvableType sourceType = typePair.getFirst();
        assertThat(sourceType, notNullValue());
        assertThat(sourceType.getType(), is(Integer.class));
        assertThat(sourceType.getComponentType(), nullValue());
        assertThat(sourceType.isCollectionLike(), is(false));
        ResolvableType targetType = typePair.getSecond();
        assertThat(targetType, notNullValue());
        assertThat(targetType.getType(), is(String.class));
        assertThat(targetType.getComponentType(), nullValue());
        assertThat(targetType.isCollectionLike(), is(false));
    }

    @Test
    void resolveConvertiblePairWhenClassImplementsMultipleInterfacesFieldConverterThenResolves() {
        Pair<ResolvableType, ResolvableType> typePair = ConversionUtils
                .resolveConvertiblePair(ClassImplementsMultipleInterfacesFieldConverter.class, FieldConverter.class);
        ResolvableType sourceType = typePair.getFirst();
        assertThat(sourceType, notNullValue());
        assertThat(sourceType.getType(), is(Integer.class));
        assertThat(sourceType.getComponentType(), nullValue());
        assertThat(sourceType.isCollectionLike(), is(false));
        ResolvableType targetType = typePair.getSecond();
        assertThat(targetType, notNullValue());
        assertThat(targetType.getType(), is(String.class));
        assertThat(targetType.getComponentType(), nullValue());
        assertThat(targetType.isCollectionLike(), is(false));
    }

    @Test
    void resolveConvertiblePairWhenClassExtendsClassImplementingFieldConverterThenResolves() {
        Pair<ResolvableType, ResolvableType> typePair = ConversionUtils
                .resolveConvertiblePair(ClassExtendsClassImplementingFieldConverter.class, FieldConverter.class);
        ResolvableType sourceType = typePair.getFirst();
        assertThat(sourceType, notNullValue());
        assertThat(sourceType.getType(), is(Integer.class));
        assertThat(sourceType.getComponentType(), nullValue());
        assertThat(sourceType.isCollectionLike(), is(false));
        ResolvableType targetType = typePair.getSecond();
        assertThat(targetType, notNullValue());
        assertThat(targetType.getType(), is(String.class));
        assertThat(targetType.getComponentType(), nullValue());
        assertThat(targetType.isCollectionLike(), is(false));
    }

    @Test
    void resolveConvertiblePairWhenClassExtendsClassImplementingMultipleInterfacesFieldConverterThenResolves() {
        Pair<ResolvableType, ResolvableType> typePair = ConversionUtils
                .resolveConvertiblePair(ClassExtendsClassImplementingMultipleInterfacesFieldConverter.class, FieldConverter.class);
        ResolvableType sourceType = typePair.getFirst();
        assertThat(sourceType, notNullValue());
        assertThat(sourceType.getType(), is(Integer.class));
        assertThat(sourceType.getComponentType(), nullValue());
        assertThat(sourceType.isCollectionLike(), is(false));
        ResolvableType targetType = typePair.getSecond();
        assertThat(targetType, notNullValue());
        assertThat(targetType.getType(), is(String.class));
        assertThat(targetType.getComponentType(), nullValue());
        assertThat(targetType.isCollectionLike(), is(false));
    }

    @Test
    void resolveConvertiblePairWhenClassExtendsGenericConverterThenResolves() {
        Pair<ResolvableType, ResolvableType> typePair = ConversionUtils
                .resolveConvertiblePair(ClassExtendsGenericFieldConverter.class, FieldConverter.class);
        ResolvableType sourceType = typePair.getFirst();
        assertThat(sourceType, notNullValue());
        assertThat(sourceType.getType(), is(Integer.class));
        assertThat(sourceType.getComponentType(), nullValue());
        assertThat(sourceType.isCollectionLike(), is(false));
        ResolvableType targetType = typePair.getSecond();
        assertThat(targetType, notNullValue());
        assertThat(targetType.getType(), is(String.class));
        assertThat(targetType.getComponentType(), nullValue());
        assertThat(targetType.isCollectionLike(), is(false));
    }

    @Test
    void resolveConvertiblePairWhenClassExtendsClassExtendingGenericFieldConverterThenResolves() {
        Pair<ResolvableType, ResolvableType> typePair = ConversionUtils
                .resolveConvertiblePair(ClassExtendsClassExtendingGenericFieldConverter.class, FieldConverter.class);
        ResolvableType sourceType = typePair.getFirst();
        assertThat(sourceType, notNullValue());
        assertThat(sourceType.getType(), is(Integer.class));
        assertThat(sourceType.getComponentType(), nullValue());
        assertThat(sourceType.isCollectionLike(), is(false));
        ResolvableType targetType = typePair.getSecond();
        assertThat(targetType, notNullValue());
        assertThat(targetType.getType(), is(String.class));
        assertThat(targetType.getComponentType(), nullValue());
        assertThat(targetType.isCollectionLike(), is(false));
    }

    @Test
    void resolveConvertiblePairWhenContainerTypeListFieldConverterThenResolves() {
        Pair<ResolvableType, ResolvableType> typePair = ConversionUtils
                .resolveConvertiblePair(ContainerTypeListFieldConverter.class, FieldConverter.class);
        ResolvableType sourceType = typePair.getFirst();
        assertThat(sourceType, notNullValue());
        assertThat(sourceType.getType(), is(List.class));
        assertThat(sourceType.getComponentType(), is(Integer.class));
        assertThat(sourceType.isCollectionLike(), is(true));
        ResolvableType targetType = typePair.getSecond();
        assertThat(targetType, notNullValue());
        assertThat(targetType.getType(), is(List.class));
        assertThat(targetType.getComponentType(), is(String.class));
        assertThat(targetType.isCollectionLike(), is(true));
    }

    @Test
    void resolveConvertiblePairWhenContainerTypeSetFieldConverterThenResolves() {
        Pair<ResolvableType, ResolvableType> typePair = ConversionUtils
                .resolveConvertiblePair(ContainerTypeSetFieldConverter.class, FieldConverter.class);
        ResolvableType sourceType = typePair.getFirst();
        assertThat(sourceType, notNullValue());
        assertThat(sourceType.getType(), is(List.class));
        assertThat(sourceType.getComponentType(), is(Integer.class));
        assertThat(sourceType.isCollectionLike(), is(true));
        ResolvableType targetType = typePair.getSecond();
        assertThat(targetType, notNullValue());
        assertThat(targetType.getType(), is(Set.class));
        assertThat(targetType.getComponentType(), is(String.class));
        assertThat(targetType.isCollectionLike(), is(true));
    }

    @Test
    void resolveConvertiblePairWhenContainerTypeCollectionFieldConverterThenResolves() {
        Pair<ResolvableType, ResolvableType> typePair = ConversionUtils
                .resolveConvertiblePair(ContainerTypeCollectionFieldConverter.class, FieldConverter.class);
        ResolvableType sourceType = typePair.getFirst();
        assertThat(sourceType, notNullValue());
        assertThat(sourceType.getType(), is(Collection.class));
        assertThat(sourceType.getComponentType(), is(Integer.class));
        assertThat(sourceType.isCollectionLike(), is(true));
        ResolvableType targetType = typePair.getSecond();
        assertThat(targetType, notNullValue());
        assertThat(targetType.getType(), is(Collection.class));
        assertThat(targetType.getComponentType(), is(String.class));
        assertThat(targetType.isCollectionLike(), is(true));
    }

    @Test
    void resolveConvertiblePairWhenArrayTypeFieldConverterThenResolves() {
        Pair<ResolvableType, ResolvableType> typePair = ConversionUtils
                .resolveConvertiblePair(ArrayTypeFieldConverter.class, FieldConverter.class);
        ResolvableType sourceType = typePair.getFirst();
        assertThat(sourceType, notNullValue());
        assertThat(sourceType.getType(), is(Integer[].class));
        assertThat(sourceType.getComponentType(), is(Integer.class));
        assertThat(sourceType.isCollectionLike(), is(true));
        ResolvableType targetType = typePair.getSecond();
        assertThat(targetType, notNullValue());
        assertThat(targetType.getType(), is(String[].class));
        assertThat(targetType.getComponentType(), is(String.class));
        assertThat(targetType.isCollectionLike(), is(true));
    }

    @Test
    void resolveConvertiblePairWhenContainerSourceTypeOptionalFieldConverterThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ConversionUtils.resolveConvertiblePair(ContainerSourceTypeOptionalFieldConverter.class, FieldConverter.class));
        String expectedMessage = "Cannot resolve source type: java.util.Optional<java.lang.String> of converter type: " + FieldConverter.class.getName()
                                 + " for converter class: " + ContainerSourceTypeOptionalFieldConverter.class.getName();
        assertThat(exception.getMessage(), is(expectedMessage));
    }

    @Test
    void resolveConvertiblePairWhenContainerSourceTypeRawCollectionFieldConverterThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ConversionUtils.resolveConvertiblePair(ContainerSourceTypeRawCollectionFieldConverter.class, FieldConverter.class));
        String expectedMessage = "Cannot resolve source type: interface java.util.Collection of converter type: " + FieldConverter.class.getName()
                                 + " for converter class: " + ContainerSourceTypeRawCollectionFieldConverter.class.getName();
        assertThat(exception.getMessage(), is(expectedMessage));
    }

    @Test
    void resolveConvertiblePairWhenContainerSourceTypeGenericCollectionFieldConverterThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ConversionUtils.resolveConvertiblePair(ContainerSourceTypeGenericCollectionFieldConverter.class, FieldConverter.class));
        String expectedMessage = "Cannot resolve source type: java.util.Collection<T> of converter type: " + FieldConverter.class.getName()
                                 + " for converter class: " + ContainerSourceTypeGenericCollectionFieldConverter.class.getName();
        assertThat(exception.getMessage(), is(expectedMessage));
    }

    @Test
    void resolveConvertiblePairWhenContainerSourceTypeWildcardCollectionFieldConverterThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ConversionUtils.resolveConvertiblePair(ContainerSourceTypeWildcardCollectionFieldConverter.class, FieldConverter.class));
        String expectedMessage = "Cannot resolve source type: java.util.Collection<?> of converter type: " + FieldConverter.class.getName()
                                 + " for converter class: " + ContainerSourceTypeWildcardCollectionFieldConverter.class.getName();
        assertThat(exception.getMessage(), is(expectedMessage));
    }

    @Test
    void resolveConvertiblePairWhenContainerSourceTypeGenericArrayFieldConverterThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ConversionUtils.resolveConvertiblePair(ContainerSourceTypeGenericArrayFieldConverter.class, FieldConverter.class));
        String expectedMessage = "Cannot resolve source type: T[] of converter type: " + FieldConverter.class.getName()
                                 + " for converter class: " + ContainerSourceTypeGenericArrayFieldConverter.class.getName();
        assertThat(exception.getMessage(), is(expectedMessage));
    }

    @Test
    void resolveConvertiblePairWhenContainerSourceTypeGenericFieldConverterThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ConversionUtils.resolveConvertiblePair(ContainerSourceTypeGenericFieldConverter.class, FieldConverter.class));
        String expectedMessage = "Cannot resolve source type: T of converter type: " + FieldConverter.class.getName()
                                 + " for converter class: " + ContainerSourceTypeGenericFieldConverter.class.getName();
        assertThat(exception.getMessage(), is(expectedMessage));
    }

    @Test
    void resolveConvertiblePairWhenContainerTargetTypeOptionalFieldConverterThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ConversionUtils.resolveConvertiblePair(ContainerTypeOptionalFieldConverter.class, FieldConverter.class));
        String expectedMessage = "Cannot resolve target type: java.util.Optional<java.lang.String> of converter type: " 
                                 + FieldConverter.class.getName() + " for converter class: "
                                 + ContainerTypeOptionalFieldConverter.class.getName();
        assertThat(exception.getMessage(), is(expectedMessage));
    }

    @Test
    void resolveConvertiblePairWhenContainerTargetTypeRawCollectionFieldConverterThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ConversionUtils.resolveConvertiblePair(ContainerTargetTypeRawCollectionFieldConverter.class, FieldConverter.class));
        String expectedMessage = "Cannot resolve target type: interface java.util.Collection of converter type: "
                                 + FieldConverter.class.getName() + " for converter class: "
                                 + ContainerTargetTypeRawCollectionFieldConverter.class.getName();
        assertThat(exception.getMessage(), is(expectedMessage));
    }

    @Test
    void resolveConvertiblePairWhenContainerTargetTypeGenericCollectionFieldConverterThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ConversionUtils.resolveConvertiblePair(ContainerTargetTypeGenericCollectionFieldConverter.class, FieldConverter.class));
        String expectedMessage = "Cannot resolve target type: java.util.Collection<T> of converter type: "
                                 + FieldConverter.class.getName() + " for converter class: "
                                 + ContainerTargetTypeGenericCollectionFieldConverter.class.getName();
        assertThat(exception.getMessage(), is(expectedMessage));
    }

    @Test
    void resolveConvertiblePairWhenContainerTargetTypeWildcardCollectionFieldConverterThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ConversionUtils.resolveConvertiblePair(ContainerTargetTypeWildcardCollectionFieldConverter.class, FieldConverter.class));
        String expectedMessage = "Cannot resolve target type: java.util.Collection<?> of converter type: "
                                 + FieldConverter.class.getName() + " for converter class: "
                                 + ContainerTargetTypeWildcardCollectionFieldConverter.class.getName();
        assertThat(exception.getMessage(), is(expectedMessage));
    }

    @Test
    void resolveConvertiblePairWhenContainerTargetTypeGenericArrayFieldConverterThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ConversionUtils.resolveConvertiblePair(ContainerTargetTypeGenericArrayFieldConverter.class, FieldConverter.class));
        String expectedMessage = "Cannot resolve target type: T[] of converter type: "
                                 + FieldConverter.class.getName() + " for converter class: "
                                 + ContainerTargetTypeGenericArrayFieldConverter.class.getName();
        assertThat(exception.getMessage(), is(expectedMessage));
    }

    @Test
    void resolveConvertiblePairWhenContainerTargetTypeGenericFieldConverterThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ConversionUtils.resolveConvertiblePair(ContainerTargetTypeGenericFieldConverter.class, FieldConverter.class));
        String expectedMessage = "Cannot resolve target type: T of converter type: "
                                 + FieldConverter.class.getName() + " for converter class: "
                                 + ContainerTargetTypeGenericFieldConverter.class.getName();
        assertThat(exception.getMessage(), is(expectedMessage));
    }

    @Test
    void resolveConvertiblePairWhenConverterClassNullThenException() {
        NullPointerException exception = assertThrows(NullPointerException.class,
                () -> ConversionUtils.resolveConvertiblePair(null, FieldConverter.class));
        assertThat(exception.getMessage(), is("converterClass must not be null"));
    }

    @Test
    void resolveConvertiblePairWhenConverterTypeNullThenException() {
        NullPointerException exception = assertThrows(NullPointerException.class,
                () -> ConversionUtils.resolveConvertiblePair(ClassImplementsFieldConverter.class, null));
        assertThat(exception.getMessage(), is("converterType must not be null"));
    }

    @Test
    void resolveConvertiblePairWhenSingleTypeArgumentThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ConversionUtils.resolveConvertiblePair(ClassImplementsMultipleInterfacesFieldConverter.class, Callable.class));
        String expectedMessage = "Converter type: " + Callable.class.getName() + " must have 2 type arguments";
        assertThat(exception.getMessage(), is(expectedMessage));
    }

    @Test
    void resolveFieldTypeWhenSimpleFieldThenResolves() {
        ResolvableType resolvableType = ConversionUtils.resolveFieldType(getField("simpleField"));
        assertThat(resolvableType, notNullValue());
        assertThat(resolvableType.getType(), is(String.class));
        assertThat(resolvableType.getComponentType(), nullValue());
        assertThat(resolvableType.isCollectionLike(), is(false));
    }

    @Test
    void resolveFieldTypeWhenListFieldThenResolves() {
        ResolvableType resolvableType = ConversionUtils.resolveFieldType(getField("listField"));
        assertThat(resolvableType, notNullValue());
        assertThat(resolvableType.getType(), is(List.class));
        assertThat(resolvableType.getComponentType(), is(String.class));
        assertThat(resolvableType.isCollectionLike(), is(true));
    }

    @Test
    void resolveFieldTypeWhenSetFieldThenResolves() {
        ResolvableType resolvableType = ConversionUtils.resolveFieldType(getField("setField"));
        assertThat(resolvableType, notNullValue());
        assertThat(resolvableType.getType(), is(Set.class));
        assertThat(resolvableType.getComponentType(), is(String.class));
        assertThat(resolvableType.isCollectionLike(), is(true));
    }

    @Test
    void resolveFieldTypeWhenCollectionFieldThenResolves() {
        ResolvableType resolvableType = ConversionUtils.resolveFieldType(getField("collectionField"));
        assertThat(resolvableType, notNullValue());
        assertThat(resolvableType.getType(), is(Collection.class));
        assertThat(resolvableType.getComponentType(), is(String.class));
        assertThat(resolvableType.isCollectionLike(), is(true));
    }

    @Test
    void resolveFieldTypeWhenArrayFieldThenResolves() {
        ResolvableType resolvableType = ConversionUtils.resolveFieldType(getField("arrayField"));
        assertThat(resolvableType, notNullValue());
        assertThat(resolvableType.getType(), is(String[].class));
        assertThat(resolvableType.getComponentType(), is(String.class));
        assertThat(resolvableType.isCollectionLike(), is(true));
    }

    @Test
    void resolveFieldTypeWhenOptionalFieldThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ConversionUtils.resolveFieldType(getField("optionalField")));
        String expectedMessage = "Cannot resolve Field: " + TestPojo.class.getName() + ".optionalField"
                                 + " target type: java.util.Optional<java.lang.String>";
        assertThat(exception.getMessage(), is(expectedMessage));
    }

    @Test
    void resolveFieldTypeWhenRawCollectionFieldThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ConversionUtils.resolveFieldType(getField("rawCollectionField")));
        String expectedMessage = "Cannot resolve Field: " + TestPojo.class.getName() + ".rawCollectionField"
                                 + " target type: interface java.util.Collection";
        assertThat(exception.getMessage(), is(expectedMessage));
    }

    @Test
    void resolveFieldTypeWhenGenericCollectionFieldThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ConversionUtils.resolveFieldType(getField("genericCollectionField")));
        String expectedMessage = "Cannot resolve Field: " + TestPojo.class.getName() + ".genericCollectionField"
                                 + " target type: java.util.Collection<T>";
        assertThat(exception.getMessage(), is(expectedMessage));
    }

    @Test
    void resolveFieldTypeWhenWildcardCollectionFieldThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ConversionUtils.resolveFieldType(getField("wildcardCollectionField")));
        String expectedMessage = "Cannot resolve Field: " + TestPojo.class.getName() + ".wildcardCollectionField"
                                 + " target type: java.util.Collection<?>";
        assertThat(exception.getMessage(), is(expectedMessage));
    }

    @Test
    void resolveFieldTypeWhenGenericArrayFieldThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ConversionUtils.resolveFieldType(getField("genericArrayField")));
        String expectedMessage = "Cannot resolve Field: " + TestPojo.class.getName() + ".genericArrayField"
                                 + " target type: T[]";
        assertThat(exception.getMessage(), is(expectedMessage));
    }

    @Test
    void resolveFieldTypeWhenGenericFieldThenException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ConversionUtils.resolveFieldType(getField("genericField")));
        String expectedMessage = "Cannot resolve Field: " + TestPojo.class.getName() + ".genericField"
                                 + " target type: T";
        assertThat(exception.getMessage(), is(expectedMessage));
    }

    @Test
    void resolveFieldTypeWhenFieldNullThenException() {
        NullPointerException exception = assertThrows(NullPointerException.class,
                () -> ConversionUtils.resolveFieldType(null));
        assertThat(exception.getMessage(), is("field must not be null"));
    }

    private static Field getField(String name) {
        return FieldUtils.getDeclaredField(TestPojo.class, name, true);
    }

    static class ClassImplementsFieldConverter implements FieldConverter<Integer, String> {

        @Override
        public Integer convertToFieldType(String dbData) {
            return 0;
        }

        @Override
        public String convertToDatabaseType(Integer field) {
            return "";
        }
    }

    static class ClassImplementsMultipleInterfacesFieldConverter implements Supplier<String>, Callable<String>, FieldConverter<Integer, String> {

        @Override
        public Integer convertToFieldType(String dbData) {
            return 0;
        }

        @Override
        public String convertToDatabaseType(Integer field) {
            return "";
        }

        @Override
        public String get() {
            return "";
        }

        @Override
        public String call() {
            return "";
        }
    }

    static class ClassExtendsClassImplementingFieldConverter extends ClassImplementsFieldConverter {
    }

    static class ClassExtendsClassImplementingMultipleInterfacesFieldConverter extends ClassImplementsMultipleInterfacesFieldConverter {
    }

    static class GenericFieldConverter<X, Y> implements FieldConverter<X, Y> {

        @Override
        public X convertToFieldType(Y dbData) {
            return null;
        }

        @Override
        public Y convertToDatabaseType(X field) {
            return null;
        }
    }

    static class ClassExtendsGenericFieldConverter extends GenericFieldConverter<Integer, String> {
    }

    static class ClassExtendsClassExtendingGenericFieldConverter extends ClassExtendsGenericFieldConverter {
    }

    static class ContainerTypeListFieldConverter extends GenericFieldConverter<List<Integer>, List<String>> {
    }

    static class ContainerTypeSetFieldConverter extends GenericFieldConverter<List<Integer>, Set<String>> {
    }

    static class ContainerTypeCollectionFieldConverter extends GenericFieldConverter<Collection<Integer>, Collection<String>> {
    }

    static class ArrayTypeFieldConverter extends GenericFieldConverter<Integer[], String[]> {
    }

    static class ContainerSourceTypeOptionalFieldConverter extends GenericFieldConverter<Optional<String>, List<Integer>> {
    }

    static class ContainerSourceTypeRawCollectionFieldConverter extends GenericFieldConverter<Collection, List<Integer>> {
    }

    static class ContainerSourceTypeGenericCollectionFieldConverter<T> extends GenericFieldConverter<Collection<T>, List<Integer>> {
    }

    static class ContainerSourceTypeWildcardCollectionFieldConverter extends GenericFieldConverter<Collection<?>, List<Integer>> {
    }

    static class ContainerSourceTypeGenericArrayFieldConverter<T> extends GenericFieldConverter<T[], List<Integer>> {
    }

    static class ContainerSourceTypeGenericFieldConverter<T> extends GenericFieldConverter<T, List<Integer>> {
    }

    static class ContainerTypeOptionalFieldConverter extends GenericFieldConverter<List<Integer>, Optional<String>> {
    }

    static class ContainerTargetTypeRawCollectionFieldConverter extends GenericFieldConverter<List<Integer>, Collection> {
    }

    static class ContainerTargetTypeGenericCollectionFieldConverter<T> extends GenericFieldConverter<List<Integer>, Collection<T>> {
    }

    static class ContainerTargetTypeWildcardCollectionFieldConverter extends GenericFieldConverter<List<Integer>, Collection<?>> {
    }

    static class ContainerTargetTypeGenericArrayFieldConverter<T> extends GenericFieldConverter<List<Integer>, T[]> {
    }

    static class ContainerTargetTypeGenericFieldConverter<T> extends GenericFieldConverter<List<Integer>, T> {
    }

    static class TestPojo<T> {
        String simpleField;
        List<String> listField;
        Set<String> setField;
        Collection<String> collectionField;
        String[] arrayField;
        Optional<String> optionalField;
        Collection rawCollectionField;
        Collection<T> genericCollectionField;
        Collection<?> wildcardCollectionField;
        T[] genericArrayField;
        T genericField;
    }
}
