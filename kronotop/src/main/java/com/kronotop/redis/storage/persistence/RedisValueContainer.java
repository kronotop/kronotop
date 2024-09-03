package com.kronotop.redis.storage.persistence;

import com.kronotop.redis.hash.HashFieldValue;
import com.kronotop.redis.hash.HashValue;
import com.kronotop.redis.string.StringValue;

public class RedisValueContainer {
    private final RedisValueKind kind;
    private StringValue stringValue;
    private HashValue hashValue;
    private HashFieldValue hashFieldValue;

    public RedisValueContainer(StringValue value) {
        this.kind = RedisValueKind.STRING;
        this.stringValue = value;
    }

    public RedisValueContainer(HashValue value) {
        this.kind = RedisValueKind.HASH;
        this.hashValue = value;
    }

    public RedisValueContainer(HashFieldValue value) {
        this.kind = RedisValueKind.HASH_FIELD;
        this.hashFieldValue = value;
    }

    public RedisValueKind kind() {
        return kind;
    }

    public StringValue string() {
        return stringValue;
    }

    public HashValue hash() {
        return hashValue;
    }

    public HashFieldValue hashField() {
        return hashFieldValue;
    }
}
