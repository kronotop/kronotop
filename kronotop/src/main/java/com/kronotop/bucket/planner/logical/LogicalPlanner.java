package com.kronotop.bucket.planner.logical;

import com.kronotop.kql.KqlOperator;

import java.util.List;

public class LogicalPlanner {
    private final List<KqlOperator> operators;

    public LogicalPlanner(List<KqlOperator> operators) {
        this.operators = operators;
    }

    void plan() {
        for (KqlOperator operator : operators) {
            System.out.println(operator);
        }
    }
}
