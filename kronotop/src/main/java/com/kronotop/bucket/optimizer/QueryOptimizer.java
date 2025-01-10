package com.kronotop.bucket.optimizer;

import com.kronotop.bql.operators.BqlOperator;
import com.kronotop.bql.parser.BqlParser;

import java.util.List;

public class QueryOptimizer {
    private final String query;

    public QueryOptimizer(String query) {
        this.query = query;
    }

    public void optimize() {
        List<BqlOperator> operators = BqlParser.parse(query);
        for (BqlOperator operator : operators) {
            System.out.println(operator);
        }
    }
}
