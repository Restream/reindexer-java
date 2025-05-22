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
package ru.rt.restream.reindexer.convert;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.rt.restream.reindexer.annotations.Convert;

import java.lang.reflect.Field;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link FieldConverterRegistryFactory}.
 */
public class FieldConverterRegistryFactoryTest {

    private final FieldConverterRegistryFactory registry = FieldConverterRegistryFactory.INSTANCE;

    @AfterEach
    void tearDown() {
        registry.clearRegistry();
    }

    @Test
    void getFieldConverterWhenClassSpecifiedThenCreated() {
        FieldConverter<String, String> converter = registry
                .getFieldConverter(getField("stringLongConvertField"));
        assertThat(converter, notNullValue());
        assertThat(converter, instanceOf(StringLongFieldConverter.class));
    }

    @Test
    void getFieldConverterWhenClassSpecifiedConverterConfiguredThenConfiguredConverterTakesPrecedence() {
        StringIntegerFieldConverter converter = new StringIntegerFieldConverter();
        registry.registerFieldConverter(TestPojo.class, "stringLongConvertField", converter);
        FieldConverter<String, String> fieldConverter = registry
                .getFieldConverter(getField("stringLongConvertField"));
        assertThat(fieldConverter, notNullValue());
        assertThat(fieldConverter, sameInstance(converter));
    }

    @Test
    void getFieldConverterWhenClassSpecifiedSameGlobalConverterConfiguredThenConverterReturned() {
        StringLongFieldConverter converter = new StringLongFieldConverter();
        registry.registerGlobalConverter(converter);
        FieldConverter<String, String> fieldConverter = registry
                .getFieldConverter(getField("stringLongConvertField"));
        assertThat(fieldConverter, notNullValue());
        assertThat(fieldConverter, sameInstance(converter));
    }

    @Test
    void getFieldConverterWhenNoConvertAnnotationFieldConverterConfiguredThenConverterReturned() {
        StringLongFieldConverter converter = new StringLongFieldConverter();
        registry.registerFieldConverter(TestPojo.class, "stringField", converter);
        FieldConverter<String, String> fieldConverter = registry
                .getFieldConverter(getField("stringField"));
        assertThat(fieldConverter, notNullValue());
        assertThat(fieldConverter, sameInstance(converter));
    }

    @Test
    void getFieldConverterWhenNoConvertAnnotationGlobalConverterConfiguredThenConverterReturned() {
        StringLongFieldConverter converter = new StringLongFieldConverter();
        registry.registerGlobalConverter(converter);
        FieldConverter<String, String> fieldConverter = registry
                .getFieldConverter(getField("stringField"));
        assertThat(fieldConverter, notNullValue());
        assertThat(fieldConverter, sameInstance(converter));
    }

    @Test
    void getFieldConverterWhenClassSpecifiedGlobalConverterConfiguredThenSpecifiedTakesPrecedence() {
        StringLongFieldConverter converter = new StringLongFieldConverter();
        registry.registerGlobalConverter(converter);
        FieldConverter<String, String> fieldConverter = registry
                .getFieldConverter(getField("stringIntegerConvertField"));
        assertThat(fieldConverter, notNullValue());
        assertThat(fieldConverter, instanceOf(StringIntegerFieldConverter.class));
    }

    @Test
    void getFieldConverterWhenNoClassSpecifiedGlobalConverterConfiguredThenConverterReturned() {
        StringLongFieldConverter converter = new StringLongFieldConverter();
        registry.registerGlobalConverter(converter);
        FieldConverter<String, String> fieldConverter = registry
                .getFieldConverter(getField("stringConvertNoClassField"));
        assertThat(fieldConverter, notNullValue());
        assertThat(fieldConverter, sameInstance(converter));
    }

    @Test
    void getFieldConverterWhenNoConvertAnnotationThenNull() {
        FieldConverter<String, String> fieldConverter = registry
                .getFieldConverter(getField("stringField"));
        assertThat(fieldConverter, nullValue());
    }

    @Test
    void getFieldConverterWhenDisableConversionAndGlobalConverterConfiguredThenNull() {
        StringLongFieldConverter converter = new StringLongFieldConverter();
        registry.registerGlobalConverter(converter);
        FieldConverter<String, String> fieldConverter = registry
                .getFieldConverter(getField("stringConvertDisableField"));
        assertThat(fieldConverter, nullValue());
    }

    @Test
    void getFieldConverterWhenDisableConversionAndFieldConverterConfiguredThenNull() {
        StringLongFieldConverter converter = new StringLongFieldConverter();
        registry.registerFieldConverter(TestPojo.class, "stringConvertDisableConversionField", converter);
        FieldConverter<String, String> fieldConverter = registry
                .getFieldConverter(getField("stringConvertDisableField"));
        assertThat(fieldConverter, nullValue());
    }

    @Test
    void getFieldConverterWhenDisableConversionAndClassSpecifiedThenNull() {
        StringLongFieldConverter converter = new StringLongFieldConverter();
        registry.registerFieldConverter(TestPojo.class, "stringConvertDisableConversionClassSpecifiedField", converter);
        FieldConverter<String, String> fieldConverter = registry
                .getFieldConverter(getField("stringConvertDisableClassSpecifiedField"));
        assertThat(fieldConverter, nullValue());
    }

    @Test
    void registerFieldConverterWhenRegisteredThenOverrides() {
        FieldConverter<String, Long> stringLongFieldConverter = new StringLongFieldConverter();
        registry.registerFieldConverter(TestPojo.class, "stringField", stringLongFieldConverter);
        Field field = getField("stringField");
        FieldConverter<String, String> fieldConverter = registry.getFieldConverter(field);
        assertThat(fieldConverter, notNullValue());
        assertThat(fieldConverter, sameInstance(stringLongFieldConverter));
        StringIntegerFieldConverter stringIntegerFieldConverter = new StringIntegerFieldConverter();
        registry.registerFieldConverter(TestPojo.class, "stringField", stringIntegerFieldConverter);
        fieldConverter = registry.getFieldConverter(field);
        assertThat(fieldConverter, notNullValue());
        assertThat(fieldConverter, sameInstance(stringIntegerFieldConverter));
    }

    @Test
    void registerGlobalConverterWhenRegisteredThenOverrides() {
        FieldConverter<String, Long> stringLongFieldConverter = new StringLongFieldConverter();
        registry.registerGlobalConverter(stringLongFieldConverter);
        Field field = getField("stringField");
        FieldConverter<String, String> fieldConverter = registry
                .getFieldConverter(field);
        assertThat(fieldConverter, notNullValue());
        assertThat(fieldConverter, sameInstance(stringLongFieldConverter));
        StringIntegerFieldConverter stringIntegerFieldConverter = new StringIntegerFieldConverter();
        registry.registerGlobalConverter(stringIntegerFieldConverter);
        fieldConverter = registry.getFieldConverter(field);
        assertThat(fieldConverter, notNullValue());
        assertThat(fieldConverter, sameInstance(stringIntegerFieldConverter));
    }

    @Test
    void getFieldConverterWhenFieldNullThenException() {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> registry.getFieldConverter(null));
        assertThat(exception.getMessage(), is("field must not be null"));
    }

    @Test
    void registerFieldConverterWhenItemClassNullThenException() {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> registry
                .registerFieldConverter(null, "stringField", new StringLongFieldConverter()));
        assertThat(exception.getMessage(), is("itemClass must not be null"));
    }

    @Test
    void registerFieldConverterWhenFieldNameNullThenException() {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> registry
                .registerFieldConverter(TestPojo.class, null, new StringLongFieldConverter()));
        assertThat(exception.getMessage(), is("fieldName must not be null"));
    }

    @Test
    void registerFieldConverterWhenConverterNullThenException() {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> registry
                .registerFieldConverter(TestPojo.class, "stringField", null));
        assertThat(exception.getMessage(), is("fieldConverter must not be null"));
    }

    @Test
    void registerGlobalConverterWhenConverterNullThenException() {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> registry
                .registerGlobalConverter(null));
        assertThat(exception.getMessage(), is("converter must not be null"));
    }

    private static Field getField(String name) {
        return FieldUtils.getDeclaredField(TestPojo.class, name, true);
    }

    static class TestPojo {
        @Convert(converterClass = StringLongFieldConverter.class)
        String stringLongConvertField;
        @Convert(converterClass = StringIntegerFieldConverter.class)
        String stringIntegerConvertField;
        @Convert
        String stringConvertNoClassField;
        @Convert(disableConversion = true)
        String stringConvertDisableField;
        @Convert(disableConversion = true, converterClass = StringLongFieldConverter.class)
        String stringConvertDisableClassSpecifiedField;
        String stringField;
    }

    static class StringLongFieldConverter implements FieldConverter<String, Long> {

        @Override
        public String convertToFieldType(Long dbData) {
            return "";
        }

        @Override
        public Long convertToDatabaseType(String field) {
            return 0L;
        }
    }

    static class StringIntegerFieldConverter implements FieldConverter<String, Integer> {

        @Override
        public String convertToFieldType(Integer dbData) {
            return "";
        }

        @Override
        public Integer convertToDatabaseType(String field) {
            return 0;
        }
    }
}
