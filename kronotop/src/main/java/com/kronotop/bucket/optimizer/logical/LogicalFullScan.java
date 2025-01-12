package com.kronotop.bucket.optimizer.logical;

public class LogicalFullScan extends LogicalNode {

    @Override
    public String toString() {
        return "LogicalFullScan [filters=" + filters + "]";
    }
}
