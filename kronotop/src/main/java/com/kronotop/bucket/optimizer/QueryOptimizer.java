package com.kronotop.bucket.optimizer;

import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;
import com.kronotop.bucket.bql.operators.comparison.BqlEqOperator;
import com.kronotop.bucket.bql.operators.logical.BqlOrOperator;
import com.kronotop.bucket.bql.parser.BqlParser;
import com.kronotop.bucket.optimizer.logical.*;

import java.util.LinkedList;
import java.util.List;

public class QueryOptimizer {
    private final String query;
    private List<BqlOperator> operators;
    private List<LogicalFilter> filters = new LinkedList<>();

    public QueryOptimizer(String query) {
        this.query = query;
    }

    private int traverse(LogicalNode root, BqlEqOperator operator, int index) {
        for (int i = index; i < operators.size(); i++) {
            BqlOperator child = operators.get(i);
            if (child.getLevel() <= operator.getLevel()) {
                return i - 1;
            }
            if (child.getOperatorType().equals(OperatorType.EQ)) {
                LogicalEqFilter eqFilter = new LogicalEqFilter();
                eqFilter.setField(operator.getField());
                child.getValues().forEach(eqFilter::addValue);
                root.addFilter(eqFilter);
            } else if (child.getOperatorType().equals(OperatorType.LT)) {
                LogicalLtFilter ltFilter = new LogicalLtFilter();
                ltFilter.setField(operator.getField());
                child.getValues().forEach(ltFilter::addValue);
                root.addFilter(ltFilter);
            } else if (child.getOperatorType().equals(OperatorType.GT)) {
                LogicalGtFilter gtFilter = new LogicalGtFilter();
                gtFilter.setField(operator.getField());
                child.getValues().forEach(gtFilter::addValue);
                root.addFilter(gtFilter);
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
                i =  traverse(root, (BqlEqOperator) child, i + 1);
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
                    // Try to find sub operators
                    i = traverse(logicalScan, eq, i + 1);
                    if (i == 0) {
                        break;
                    }
                } else {
                    LogicalEqFilter root = new LogicalEqFilter();
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

// LogicalAndFilter
//    {
//       filters=[
//           LogicalEqFilter {field=status, value=BqlValue { type=STRING, value=ALIVE } },
//           LogicalEqFilter {field=username, value=BqlValue { type=STRING, value=kronotop-admin } },
//           LogicalLtFilter {field=age, value=BqlValue { type=INT32, value=35 } }
//       ]
//    }
