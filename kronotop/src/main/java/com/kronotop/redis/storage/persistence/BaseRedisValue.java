package com.kronotop.redis.storage.persistence;

import com.apple.foundationdb.tuple.Versionstamp;

public class BaseRedisValue<T> {
    private final T value;
    private Versionstamp versionstamp;

    public BaseRedisValue(T value) {
        this.value = value;
    }

    public T value() {
        return value;
    }

    public void setVersionstamp(Versionstamp versionstamp) {
        this.versionstamp = versionstamp;
    }

    public Versionstamp versionstamp() {
        return versionstamp;
    }
}
