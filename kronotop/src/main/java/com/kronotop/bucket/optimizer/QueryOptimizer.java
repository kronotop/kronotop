package com.kronotop.bucket.optimizer;

import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.parser.BqlParser;

import java.util.List;

public class QueryOptimizer {
    private final String query;

    public QueryOptimizer(String query) {
        this.query = query;
    }

    public void optimize() {
        List<BqlOperator> operators = BqlParser.parse(query);
        int lowestLevel = 0;
        for (BqlOperator operator : operators) {

        }
    }
}
