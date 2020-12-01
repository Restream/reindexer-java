package ru.rt.restream.reindexer;

public enum  CollateMode {
    NUMERIC("numeric"), ASCII("ascii"), UTF8("utf8"), CUSTOM("custom"), NONE("");

    private final String name;

    CollateMode(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
