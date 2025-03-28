// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.planner.logical;

import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;
import com.kronotop.bucket.bql.operators.comparison.BqlEqOperator;
import com.kronotop.bucket.bql.parser.BqlParser;

import java.util.List;

public class LogicalPlanner {
    private final String bucket;
    private final String query;
    private List<BqlOperator> bqlOperators;

    public LogicalPlanner(String bucket, String query) {
        this.bucket = bucket;
        this.query = query;
    }

    public String getBucket() {
        return bucket;
    }

    private int traverse(LogicalNode root, BqlEqOperator operator, int index) {
        for (int i = index; i < bqlOperators.size(); i++) {
            BqlOperator bqlOperator = bqlOperators.get(i);
            if (bqlOperator.getLevel() <= operator.getLevel()) {
                return i;
            }
            switch (bqlOperator.getOperatorType()) {
                case EQ, LT, GT, GTE, NE:
                    LogicalComparisonFilter filter = new LogicalComparisonFilter(bqlOperator.getOperatorType());
                    filter.setField(operator.getField());
                    bqlOperator.getValues().forEach(filter::addValue);
                    root.addFilter(filter);
                    break;
                case EXISTS:
                    LogicalExistsFilter existsFilter = new LogicalExistsFilter();
                    existsFilter.setField(operator.getField());
                    root.addFilter(existsFilter);
                    break;
                case ALL:
                    LogicalAndFilter andOperator = new LogicalAndFilter();
                    bqlOperator.getValues().forEach(bqlValue -> {
                        LogicalComparisonFilter eqFilter = new LogicalComparisonFilter(OperatorType.EQ);
                        eqFilter.setField(operator.getField());
                        eqFilter.addValue(bqlValue);
                        andOperator.addFilter(eqFilter);
                    });
                    root.addFilter(andOperator);
            }
        }
        return 0;
    }

    private int traverse1(LogicalNode root, int level, int index) {
        int idx = index;
        while (idx < bqlOperators.size()) {
            BqlOperator child = bqlOperators.get(idx);
            if (child.getLevel() <= level) {
                return idx;
            }
            idx = traverse0(idx, root);
            if (idx == 0) {
                break;
            }
        }
        return 0;
    }

    private int traverse0(int idx, LogicalNode root) {
        BqlOperator operator = bqlOperators.get(idx);
        if (operator.getOperatorType().equals(OperatorType.EQ)) {
            BqlEqOperator eq = (BqlEqOperator) operator;
            if (eq.getValues() == null) {
                return traverse(root, eq, idx + 1);
            } else {
                LogicalComparisonFilter comparisonFilter = new LogicalComparisonFilter(OperatorType.EQ);
                comparisonFilter.setField(eq.getField());
                eq.getValues().forEach(comparisonFilter::addValue);
                root.addFilter(comparisonFilter);
            }
        } else if (operator.getOperatorType().equals(OperatorType.OR) || operator.getOperatorType().equals(OperatorType.AND)) {
            LogicalNode newRoot = makeRootNode(operator.getOperatorType());
            idx = traverse1(newRoot, operator.getLevel(), idx + 1);
            root.addFilter(newRoot);
            return idx;
        }
        return idx+1;
    }

    private LogicalNode makeRootNode(OperatorType operatorType) {
        return switch (operatorType) {
            case AND -> new LogicalAndFilter();
            case OR -> new LogicalOrFilter();
            default -> throw new IllegalArgumentException("Unsupported operator type: " + operatorType);
        };
    }

    public LogicalNode plan() {
        bqlOperators = BqlParser.parse(query);

        LogicalFullScan logicalScan = new LogicalFullScan(bucket);
        int idx = 0;
        while (idx < bqlOperators.size()) {
            idx = traverse0(idx, logicalScan);
            if (idx == 0) {
                break;
            }
        }

        return logicalScan;
    }
}