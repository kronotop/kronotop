package com.kronotop.bucket.optimizer;

public enum NodeKind {
    FULL_BUCKET_SCAN(0);

    final int value;

    NodeKind(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
