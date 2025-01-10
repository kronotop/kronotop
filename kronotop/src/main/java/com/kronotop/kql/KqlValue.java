package com.kronotop.kql;

import org.bson.BsonType;

public class KqlValue<T> {
    private final BsonType type;
    private T value;

    public KqlValue(BsonType type) {
        this.type = type;
    }

    public BsonType bsonType() {
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
        return "KqlValue [type=" + type + ", value=" + value + "]";
    }
}
