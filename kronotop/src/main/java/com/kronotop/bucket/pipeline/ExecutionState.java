package com.kronotop.bucket.pipeline;

public class ExecutionState {
    private volatile Bound lower;
    private volatile Bound upper;
    private volatile int limit;

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
        this.limit = limit;
    }

    public int getLimit() {
        return limit;
    }
}
