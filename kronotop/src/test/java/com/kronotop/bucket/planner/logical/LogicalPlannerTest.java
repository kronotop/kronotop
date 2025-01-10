package com.kronotop.bucket.planner.logical;

import com.kronotop.kql.operators.KqlOperator;
import com.kronotop.kql.parser.KqlParser;
import org.junit.jupiter.api.Test;

import java.util.List;

class LogicalPlannerTest {
    @Test
    void test() {
        List<KqlOperator> operators = KqlParser.parse("{ $or: [ { status: {$eq: 'A' } }, { qty: { $lt: 30 } } ], username: { $eq: 'buraksezer' } }");
        LogicalPlanner planner = new LogicalPlanner(operators);
        planner.plan();
    }
}