package com.kronotop.bucket.optimizer;

import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;
import com.kronotop.bucket.bql.operators.comparison.BqlEqOperator;
import com.kronotop.bucket.bql.operators.logical.BqlOrOperator;
import com.kronotop.bucket.bql.parser.BqlParser;
import com.kronotop.bucket.optimizer.logical.*;

import java.util.List;

public class QueryOptimizer {
    private final String query;
    private List<BqlOperator> operators;

    public QueryOptimizer(String query) {
        this.query = query;
    }

    private int traverse(LogicalNode root, BqlEqOperator operator, int index) {
        for (int i = index; i < operators.size(); i++) {
            BqlOperator child = operators.get(i);
            if (child.getLevel() <= operator.getLevel()) {
                return i - 1;
            }
            switch (child.getOperatorType()) {
                case EQ, LT, GT:
                    LogicalComparisonFilter filter = new LogicalComparisonFilter(child.getOperatorType());
                    filter.setField(operator.getField());
                    child.getValues().forEach(filter::addValue);
                    root.addFilter(filter);
                    break;
                case ALL:
                    LogicalAndFilter andFilter = new LogicalAndFilter();
                    child.getValues().forEach(bqlValue -> {
                        LogicalComparisonFilter eqFilter = new LogicalComparisonFilter(OperatorType.EQ);
                        eqFilter.setField(operator.getField());
                        eqFilter.addValue(bqlValue);
                        andFilter.addFilter(eqFilter);
                    });
                    root.addFilter(andFilter);
            }
        }
        return 0;
    }

    private int traverse(LogicalNode root, BqlOrOperator operator, int index) {
        for (int i = index; i < operators.size(); i++) {
            BqlOperator child = operators.get(i);
            if (child.getLevel() <= operator.getLevel()) {
                return i - 1;
            }
            if (child.getOperatorType().equals(OperatorType.EQ)) {
                i = traverse(root, (BqlEqOperator) child, i + 1);
            }
        }
        return 0;
    }

    public LogicalNode optimize() {
        operators = BqlParser.parse(query);

        LogicalFullScan logicalScan = new LogicalFullScan();
        for (int i = 0; i < operators.size(); i++) {
            BqlOperator operator = operators.get(i);
            if (operator.getOperatorType().equals(OperatorType.EQ)) {
                BqlEqOperator eq = (BqlEqOperator) operator;
                if (eq.getValues() == null) {
                    i = traverse(logicalScan, eq, i + 1);
                    if (i == 0) {
                        break;
                    }
                } else {
                    LogicalComparisonFilter root = new LogicalComparisonFilter(OperatorType.EQ);
                    root.setField(eq.getField());
                    eq.getValues().forEach(root::addValue);
                    logicalScan.addFilter(root);
                }
            } else if (operator.getOperatorType().equals(OperatorType.OR)) {
                LogicalOrFilter root = new LogicalOrFilter();
                i = traverse(root, (BqlOrOperator) operator, i + 1);
                logicalScan.addFilter(root);

                if (i == 0) {
                    break;
                }
            }
        }

        return logicalScan;
    }
}