package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.bql.ast.BqlValue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecutionState {
    private volatile Bound lower;
    private volatile Bound upper;
    private final AtomicInteger limit = new AtomicInteger();
    private final Map<Versionstamp, BqlValue> lastProcessedIndexEntries = new ConcurrentHashMap<>();

    public void setUpper(Bound upper) {
        this.upper = upper;
    }

    public Bound getUpper() {
        return upper;
    }

    public void setLower(Bound lower) {
        this.lower = lower;
    }

    public Bound getLower() {
        return lower;
    }

    public void setLimit(int limit) {
        this.limit.set(limit);
    }

    public void tryInitializingLimit(int limit) {
        this.limit.updateAndGet((current) -> {
           if (current == 0) {
               return limit;
           }
            return current;
        });
    }

    public int getLimit() {
        return limit.get();
    }

    public void updateLastProcessedIndexEntry(Versionstamp key, BqlValue value) {
        lastProcessedIndexEntries.put(key, value);
    }

    public BqlValue getLastProcessedIndexEntry(Versionstamp key) {
        return lastProcessedIndexEntries.get(key);
    }
}
