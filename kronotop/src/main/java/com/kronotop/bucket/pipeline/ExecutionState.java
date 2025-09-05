package com.kronotop.bucket.pipeline;

import java.util.concurrent.atomic.AtomicInteger;

public class ExecutionState {
    private final AtomicInteger limit = new AtomicInteger();
    private volatile Bound lower;
    private volatile Bound upper;
    private volatile boolean exhausted;
    private volatile SelectorPair selector;

    public boolean isEmpty() {
        return upper == null && lower == null;
    }

    public Bound getUpper() {
        return upper;
    }

    public void setUpper(Bound upper) {
        this.upper = upper;
    }

    public Bound getLower() {
        return lower;
    }

    public void setLower(Bound lower) {
        this.lower = lower;
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

    public void setLimit(int limit) {
        this.limit.set(limit);
    }

    public boolean isExhausted() {
        return exhausted;
    }

    public void setExhausted(boolean exhausted) {
        this.exhausted = exhausted;
    }

    public SelectorPair getSelector() {
        return selector;
    }

    public void setSelector(SelectorPair selector) {
        this.selector = selector;
    }
}
