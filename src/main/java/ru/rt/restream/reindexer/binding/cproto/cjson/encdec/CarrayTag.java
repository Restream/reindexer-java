package ru.rt.restream.reindexer.binding.cproto.cjson.encdec;

class CarrayTag {

    public static final int COUNT_BITS = 24;

    private final long value;

    public CarrayTag(int count, int tag) {
        this.value = count | (tag << COUNT_BITS);
    }

    public CarrayTag(long value) {
        this.value = value;
    }

    public int count() {
        return (int) value & ((1 << COUNT_BITS) - 1);
    }

    public int tag() {
        return (int) value >> COUNT_BITS;
    }

    public long getValue() {
        return value;
    }

}
