package com.kronotop.bucket.planner.logical;

import com.kronotop.kql.KqlOperator;
import com.kronotop.kql.parser.KqlParser;
import org.junit.jupiter.api.Test;

import java.util.List;

class LogicalPlannerTest {
    @Test
    void test() {
        List<KqlOperator> operators = KqlParser.parse("{ $and: [ { scores: 75, name: 'Burak Sezer' } ] }");
        LogicalPlanner planner = new LogicalPlanner(operators);
        planner.plan();
    }
}