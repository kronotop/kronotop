package com.kronotop.bucket.bql;

import org.bson.BsonType;

public class BqlValue<T> {
    private final BsonType type;
    private T value;

    public BqlValue(BsonType type) {
        this.type = type;
    }

    public BsonType getBsonType() {
        return type;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "BqlValue { type=" + type + ", value=" + value + " }";
    }
}