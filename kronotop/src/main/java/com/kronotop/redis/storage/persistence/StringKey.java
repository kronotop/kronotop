package com.kronotop.redis.storage.persistence;

public class StringKey implements Key {
    private final String key;

    public StringKey(String key) {
        this.key = key;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }
}
