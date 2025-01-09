package com.kronotop.kql;

import org.bson.BsonType;

public class KqlValue {
    private final BsonType type;
    private String stringValue;
    private int int32Value;

    public KqlValue(BsonType type) {
        this.type = type;
    }

    public BsonType bsonType() {
        return type;
    }

    public String getStringValue() {
        return stringValue;
    }

    public void setStringValue(String stringValue) {
        this.stringValue = stringValue;
    }

    public int getInt32Value() {
        return int32Value;
    }

    public void setInt32Value(int int32Value) {
        this.int32Value = int32Value;
    }

    @Override
    public String toString() {
        if (type.equals(BsonType.STRING)) {
            return "KqlValue [type=" + type + ", stringValue=" + stringValue + "]";
        } else if (type.equals(BsonType.INT32)) {
            return "KqlValue [type=" + type + ", int32=" + int32Value + "]";
        }
        return "KqlValue [type=" + type + "]";
    }
}
