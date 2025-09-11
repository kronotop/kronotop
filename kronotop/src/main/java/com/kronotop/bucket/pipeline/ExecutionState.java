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

    /**
     * Initializes the execution limit for processing by updating the internal state only if it has not been set previously.
     * If the current limit is zero, it will be set to the provided value; otherwise, the limit remains unchanged.
     *
     * @param limit the maximum limit to be initialized, applicable if the current limit is zero
     */
    public void initializeLimit(int limit) {
        // Limit cannot be zero, because it means "fetch all data" in FDB jargon.
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
