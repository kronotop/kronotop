package com.kronotop.bucket.optimizer;

import com.kronotop.bucket.optimizer.logical.LogicalNode;
import org.junit.jupiter.api.Test;

class QueryOptimizerTest {
    @Test
    public void test_optimize() {
        QueryOptimizer optimizer = new QueryOptimizer("{ $or: [ { status: {$eq: 'A' } }, { qty: { $lt: 30 } } ], username: { $eq: 'buraksezer' }, tags: { $all: ['foo', 32]} }");
        //QueryOptimizer optimizer = new QueryOptimizer("{ status: {$eq: 'ALIVE'}, username: {$eq: 'kronotop-admin'}, age: {$lt: 35} }");
        //QueryOptimizer optimizer = new QueryOptimizer("{ status: 'ALIVE', username: 'kronotop-admin' }");
        //QueryOptimizer optimizer = new QueryOptimizer("{}");
        LogicalNode node = optimizer.optimize();
        System.out.println(node);
    }
}