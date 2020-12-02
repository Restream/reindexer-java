package ru.rt.restream.reindexer;

import java.util.Arrays;

public enum  IndexType {
    HASH("hash"), TREE("tree"), TEXT("text"), TTL("ttl"), RTREE("rtree"), COLUMN("-"), DEFAULT(null);

    private final String name;

    IndexType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static IndexType fromName(String name) {
        return Arrays.stream(IndexType.values())
                .filter(type -> type.name.equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No such index"));
    }
}
