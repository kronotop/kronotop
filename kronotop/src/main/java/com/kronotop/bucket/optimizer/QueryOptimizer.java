package com.kronotop.bucket.optimizer;

import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.comparison.BqlEqOperator;
import com.kronotop.bucket.bql.parser.BqlParser;
import com.kronotop.bucket.optimizer.logical.LogicalAndFilter;
import com.kronotop.bucket.optimizer.logical.LogicalEqFilter;
import com.kronotop.bucket.optimizer.logical.LogicalFilter;

import java.util.List;

public class QueryOptimizer {
    private final String query;

    public QueryOptimizer(String query) {
        this.query = query;
    }

    public void optimize() {
        List<BqlOperator> operators = BqlParser.parse(query);
        System.out.println(operators);
        LogicalAndFilter root = new LogicalAndFilter();
        // level is zero
        for (BqlOperator operator : operators) {
            switch (operator.getOperatorType()) {
                case EQ -> {
                    operator.getValues().forEach(bqlValue -> {
                        LogicalEqFilter eqFilter = new LogicalEqFilter();
                        eqFilter.addValue(bqlValue);
                        BqlEqOperator eqOperator = (BqlEqOperator) operator;
                        eqFilter.setField(eqOperator.getField());
                        root.addFilter(eqFilter);
                    });
                }
            }
        }
        System.out.println(root);
    }
}

// LogicalAndFilter
//    {
//       filters=[
//       LogicalEqFilter {field=status, value=BqlValue { type=STRING, value=ALIVE } },
//       LogicalEqFilter {field=username, value=BqlValue { type=STRING, value=kronotop-admin } }
//       ]
//    }