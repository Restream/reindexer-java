package ru.rt.restream.reindexer;

public enum  FieldType {
    BOOL("bool"), INT("int"), INT64("int64"), DOUBLE("double"), STRING("string"), COMPOSITE("composite");

    private final String name;

    FieldType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
