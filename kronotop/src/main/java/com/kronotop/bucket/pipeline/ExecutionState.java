package com.kronotop.bucket.pipeline;

import java.util.concurrent.atomic.AtomicInteger;

public class ExecutionState {
    private volatile Bound lower;
    private volatile Bound upper;
    private final AtomicInteger limit = new AtomicInteger();

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
}
