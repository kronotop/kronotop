// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.optimizer.logical;

import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;
import com.kronotop.bucket.bql.operators.comparison.BqlEqOperator;
import com.kronotop.bucket.bql.operators.logical.BqlOrOperator;
import com.kronotop.bucket.bql.parser.BqlParser;

import java.util.List;

public class LogicalPlanner {
    private final String bucket;
    private final String query;
    private List<BqlOperator> operators;

    public LogicalPlanner(String bucket, String query) {
        this.bucket = bucket;
        this.query = query;
    }

    public String getBucket() {
        return bucket;
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

    public LogicalNode plan() {
        operators = BqlParser.parse(query);

        LogicalFullBucketScan logicalScan = new LogicalFullBucketScan(bucket);
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