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
                case EQ, LT, GT, GTE:
                    LogicalComparisonFilter filter = new LogicalComparisonFilter(bqlOperator.getOperatorType());
                    filter.setField(operator.getField());
                    bqlOperator.getValues().forEach(filter::addValue);
                    root.addFilter(filter);
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

    private int traverse(LogicalNode root, int level, int index) {
        int i = index;
        while (i < bqlOperators.size()) {
            BqlOperator child = bqlOperators.get(i);
            if (child.getLevel() <= level) {
                return i;
            }
            if (child.getOperatorType().equals(OperatorType.EQ)) {
                i = traverse(root, (BqlEqOperator) child, i + 1);
                continue;
            }
            i++;
        }
        return 0;
    }

    public LogicalNode plan() {
        bqlOperators = BqlParser.parse(query);

        LogicalFullScan logicalScan = new LogicalFullScan(bucket);
        int idx = 0;
        while (idx < bqlOperators.size()) {
            BqlOperator operator = bqlOperators.get(idx);
            if (operator.getOperatorType().equals(OperatorType.EQ)) {
                BqlEqOperator eq = (BqlEqOperator) operator;
                if (eq.getValues() == null) {
                    idx = traverse(logicalScan, eq, idx + 1);
                    if (idx == 0) {
                        break;
                    }
                    continue;
                } else {
                    LogicalComparisonFilter root = new LogicalComparisonFilter(OperatorType.EQ);
                    root.setField(eq.getField());
                    eq.getValues().forEach(root::addValue);
                    logicalScan.addFilter(root);
                }
            } else if (operator.getOperatorType().equals(OperatorType.OR)) {
                LogicalOrFilter root = new LogicalOrFilter();
                idx = traverse(root, operator.getLevel(), idx + 1);
                logicalScan.addFilter(root);
                if (idx == 0) {
                    break;
                }
                continue;
            } else if (operator.getOperatorType().equals(OperatorType.AND)) {
                LogicalAndFilter root = new LogicalAndFilter();
                idx = traverse(root, operator.getLevel(), idx + 1);
                logicalScan.addFilter(root);
                if (idx == 0) {
                    break;
                }
                continue;
            }
            idx++;
        }

        return logicalScan;
    }
}