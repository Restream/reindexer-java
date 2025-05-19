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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.rt.restream.reindexer.annotations.Convert;
import ru.rt.restream.reindexer.convert.util.ConversionUtils;
import ru.rt.restream.reindexer.convert.util.ResolvableType;
import ru.rt.restream.reindexer.util.Pair;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * For internal use only, as this contract is likely to change.
 */
public enum FieldConverterRegistryFactory implements FieldConverterRegistry {
    INSTANCE;

    private static final Logger LOGGER = LoggerFactory.getLogger(FieldConverterRegistryFactory.class);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Map<Class<?>, FieldConverter<?, ?>> converters = new HashMap<>();

    private final Map<ResolvableType, FieldConverter<?, ?>> globalConverters = new HashMap<>();

    private final Map<Pair<Class<?>, String>, FieldConverter<?, ?>> fieldConverters = new HashMap<>();

    /**
     * Returns a {@link FieldConverter} that is mapped for the given {@code field} or {@literal null}.
     * If the {@code FieldConverter} is not mapped directly for the field, looks up for a global one registered via
     * {@link FieldConverterRegistry#registerGlobalConverter(FieldConverter)}, if global {@code FieldConverter} is found checks
     * the {@code converterClass} from {@link Convert} annotation if its type is not default and does not match
     * the global converter's type, then specified in the annotation converter takes precedence and
     * is created for the {@code field}.
     * This method returns {@literal null} if {@code Convert} annotation's attribute {@code disableConversion} is
     * set to {@literal true} or if none of the checks above resulted in finding an eligible field converter. 
     * @param field the {@link Field} to use
     * @return the {@link FieldConverter} to use
     */
    @SuppressWarnings("unchecked")
    public <X, Y> FieldConverter<X, Y> getFieldConverter(Field field) {
        Objects.requireNonNull(field, "field must not be null");
        Convert convert = field.getAnnotation(Convert.class);
        if (convert != null && convert.disableConversion()) {
            return null;
        }
        Pair<Class<?>, String> key = new Pair<>(field.getDeclaringClass(), field.getName());
        FieldConverter<?, ?> converter;
        lock.readLock().lock();
        try {
            converter = fieldConverters.get(key);
        } finally {
            lock.readLock().unlock();
        }
        if (converter != null) {
            /*
             * Return immediately if field converter has been found.
             * Programmatically configured converter takes precedence
             * over global or the one specified in @Convert annotation.
             */
            return (FieldConverter<X, Y>) converter;
        }
        ResolvableType fieldType = ConversionUtils.resolveFieldType(field);
        lock.readLock().lock();
        try {
            converter = globalConverters.get(fieldType);
        } finally {
            lock.readLock().unlock();
        }
        /*
         * Initialize a converter instance specified in @Convert annotation
         * if annotation is present and converterClass is not default FieldConverter.class,
         * and global converter has not been found or its class does not match the one specified
         * in the annotation. The converter specified in the annotation always takes precedence
         * over the global one.
         */
        if (convert != null && convert.converterClass() != FieldConverter.class
            && (converter == null || converter.getClass() != convert.converterClass())) {
            lock.writeLock().lock();
            try {
                converter = fieldConverters.get(key);
                if (converter != null) {
                    return (FieldConverter<X, Y>) converter;
                }
                converter = globalConverters.get(fieldType);
                if (converter == null || converter.getClass() != convert.converterClass()) {
                    converter = converters.computeIfAbsent(convert.converterClass(), this::instantiateFieldConverter);
                    fieldConverters.put(key, converter);
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
        return (FieldConverter<X, Y>) converter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerFieldConverter(Class<?> itemClass, String fieldName, FieldConverter<?, ?> converter) {
        Objects.requireNonNull(itemClass, "itemClass must not be null");
        Objects.requireNonNull(fieldName, "fieldName must not be null");
        Objects.requireNonNull(converter, "fieldConverter must not be null");
        lock.writeLock().lock();
        try {
            FieldConverter<?, ?> prev = fieldConverters.put(new Pair<>(itemClass, fieldName), converter);
            if (LOGGER.isTraceEnabled()) {
                if (prev == null) {
                    LOGGER.trace("Registered field converter {} for {}.{}", converter, itemClass.getName(), fieldName);
                } else {
                    LOGGER.trace("Converter: {} was overridden to {} for {}.{}", prev, converter, itemClass.getName(), fieldName);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerGlobalConverter(FieldConverter<?, ?> converter) {
        Objects.requireNonNull(converter, "converter must not be null");
        Pair<ResolvableType, ResolvableType> convertiblePair = converter.getConvertiblePair();
        lock.writeLock().lock();
        try {
            FieldConverter<?, ?> prev = globalConverters.put(convertiblePair.getFirst(), converter);
            if (LOGGER.isTraceEnabled()) {
                if (prev == null) {
                    LOGGER.trace("Registered global converter {} for {}", converter, convertiblePair.getFirst());
                } else {
                    LOGGER.trace("Global converter: {} was overridden to {} for {}", prev, converter, convertiblePair.getFirst());
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private FieldConverter<?, ?> instantiateFieldConverter(Class<?> converterClass) {
        try {
            Constructor<?> constructor = converterClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            return (FieldConverter<?, ?>) constructor.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
