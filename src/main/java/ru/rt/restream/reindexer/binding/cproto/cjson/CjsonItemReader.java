package ru.rt.restream.reindexer.binding.cproto.cjson;

import org.apache.commons.lang3.reflect.FieldUtils;
import ru.rt.restream.reindexer.binding.cproto.ByteBuffer;
import ru.rt.restream.reindexer.binding.cproto.ItemReader;
import ru.rt.restream.reindexer.binding.cproto.cjson.encdec.CjsonDecoder;
import ru.rt.restream.reindexer.util.BeanPropertyUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class CjsonItemReader<T> implements ItemReader<T> {

    private final Class<T> itemClass;

    private final PayloadType payloadType;

    public CjsonItemReader(Class<T> itemClass, PayloadType payloadType) {
        this.itemClass = itemClass;
        this.payloadType = payloadType;
    }

    @Override
    public T readItem(ByteBuffer buffer) {
        CjsonDecoder reader = new CjsonDecoder(payloadType, buffer);
        CjsonElement element = reader.decode();
        if (!element.isObject()) {
            throw new IllegalArgumentException("Read item is not an item");
        }
        return readObject(element.getAsCjsonObject(), itemClass);
    }

    private <V> V readObject(CjsonObject cjsonObject, Class<V> target) {
        V instance = createInstance(target);
        List<Field> fields = BeanPropertyUtils.getInheritedFields(target);
        for (Field field : fields) {
            Object value = getTargetValue(field, cjsonObject.getProperty(field.getName()));
            if (value != null) {
                writeField(instance, field, value);
            }
        }
        return instance;
    }

    private Object getTargetValue(Field field, CjsonElement property) {
        Class<?> fieldType = field.getType();
        if (property.isNull()) {
            return null;
        } else if (fieldType == List.class) {
            CjsonArray array = property.getAsCjsonArray();
            ArrayList<Object> elements = new ArrayList<>();
            ParameterizedType genericType = (ParameterizedType) field.getGenericType();
            Type elementType = genericType.getActualTypeArguments()[0];
            for (CjsonElement cjsonElement : array) {
                elements.add(convert(cjsonElement, (Class<?>) elementType));
            }
            return elements;
        } else {
            return convert(property, field.getType());
        }
    }

    private Object convert(CjsonElement element, Class<?> targetClass) {
        if (element.isNull()) {
            return null;
        } else if (targetClass == Integer.class || targetClass == int.class) {
            return element.getAsInteger();
        } else if (targetClass == Long.class || targetClass == long.class) {
            return element.getAsLong();
        } else if (targetClass == Short.class || targetClass == short.class) {
            return element.getAsShort();
        } else if (targetClass == Byte.class || targetClass == byte.class) {
            return element.getAsByte();
        } else if (targetClass == Boolean.class || targetClass == boolean.class) {
            return element.getAsBoolean();
        } else if (targetClass == String.class) {
            return element.getAsString();
        } else if (targetClass == Double.class || targetClass == double.class) {
            return element.getAsDouble();
        } else if (targetClass == Float.class || targetClass == float.class) {
            return element.getAsFloat();
        } else if (element.isObject()) {
            return readObject(element.getAsCjsonObject(), targetClass);
        } else {
            throw new UnsupportedOperationException(String.format("Unsupported data type: %s", targetClass.getName()));
        }
    }

    private <V> V createInstance(Class<V> itemClass) {
        try {
            Constructor<V> constructor = itemClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            return constructor.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void writeField(Object instance, Field field, Object value) {
        try {
            FieldUtils.writeField(field, instance, value, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
