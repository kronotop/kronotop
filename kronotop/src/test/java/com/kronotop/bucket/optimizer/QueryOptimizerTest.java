package com.kronotop.bucket.optimizer;

import org.junit.jupiter.api.Test;

class QueryOptimizerTest {
    @Test
    public void test_optimize() {
        QueryOptimizer optimizer = new QueryOptimizer("{ status: {$eq: 'ALIVE'}, username: {$eq: 'kronotop-admin'} }");
        optimizer.optimize();
    }
}