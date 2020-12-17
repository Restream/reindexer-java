package ru.rt.restream.reindexer.binding.cproto.cjson;

/**
 * A class representing a Cjson primitive value.
 *
 * @author Inderjeet Singh
 * @author Joel Leitch
 */
public class CjsonPrimitive extends CjsonElement {

    private final Object value;

    public CjsonPrimitive(Object value) {
        this.value = value;
    }

    @Override
    public Boolean getAsBoolean() {
        if (value instanceof Long) {
            return (Long) value == 1L;
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        } else if (value instanceof Double) {
            return (Double) value == 1.0D;
        } else if (value instanceof Boolean) {
            return (Boolean) value;
        } else {
            throw new IllegalStateException(String.format("Unexpected value type: %s", value.getClass().getName()));
        }
    }

    @Override
    public String getAsString() {
        return String.valueOf(value);
    }

    @Override
    public Double getAsDouble() {
        if (value instanceof Long) {
            return ((Long) value).doubleValue();
        } else if (value instanceof String) {
            return Double.parseDouble((String) value);
        } else if (value instanceof Double) {
            return (Double) value;
        } else if (value instanceof Boolean) {
            return (Boolean) value ? 1.0D : 0D;
        } else {
            throw new IllegalStateException(String.format("Unexpected value type: %s", value.getClass().getName()));
        }
    }

    @Override
    public Float getAsFloat() {
        if (value instanceof Long) {
            return ((Long) value).floatValue();
        } else if (value instanceof String) {
            return Float.parseFloat((String) value);
        } else if (value instanceof Float) {
            return (Float) value;
        } else if (value instanceof Boolean) {
            return (Boolean) value ? 1.0f : 0f;
        } else {
            throw new IllegalStateException(String.format("Unexpected value type: %s", value.getClass().getName()));
        }
    }

    @Override
    public Long getAsLong() {
        if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof String) {
            return Long.parseLong((String) value);
        } else if (value instanceof Double) {
            return ((Double) value).longValue();
        } else if (value instanceof Boolean) {
            return (Boolean) value ? 1L : 0L;
        } else {
            throw new IllegalStateException(String.format("Unexpected value type: %s", value.getClass().getName()));
        }
    }

    @Override
    public Integer getAsInteger() {
        if (value instanceof Long) {
            return ((Long) value).intValue();
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        } else if (value instanceof Double) {
            return ((Double) value).intValue();
        } else if (value instanceof Boolean) {
            return (Boolean) value ? 1 : 0;
        } else {
            throw new IllegalStateException(String.format("Unexpected value type: %s", value.getClass().getName()));
        }
    }

    @Override
    public Byte getAsByte() {
        if (value instanceof Long) {
            return ((Long) value).byteValue();
        } else if (value instanceof String) {
            return Byte.parseByte((String) value);
        } else if (value instanceof Double) {
            return ((Double) value).byteValue();
        } else if (value instanceof Boolean) {
            return (byte) ((Boolean) value ? 1 : 0);
        } else {
            throw new IllegalStateException(String.format("Unexpected value type: %s", value.getClass().getName()));
        }
    }

    @Override
    public Short getAsShort() {
        if (value instanceof Long) {
            return ((Long) value).shortValue();
        } else if (value instanceof String) {
            return Short.parseShort((String) value);
        } else if (value instanceof Double) {
            return ((Double) value).shortValue();
        } else if (value instanceof Boolean) {
            return (short) ((Boolean) value ? 1 : 0);
        } else {
            throw new IllegalStateException(String.format("Unexpected value type: %s", value.getClass().getName()));
        }
    }
}
