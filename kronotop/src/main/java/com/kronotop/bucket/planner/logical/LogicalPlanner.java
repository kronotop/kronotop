/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.bucket.planner.logical;

import com.kronotop.bucket.bql.normalizer.BqlNormalizer;
import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;
import com.kronotop.bucket.bql.operators.comparison.BqlEqOperator;
import com.kronotop.bucket.bql.parser.BqlParser;
import com.kronotop.bucket.bql.validator.BqlValidator;

import java.util.List;

public class LogicalPlanner {
    private final String query;
    private List<BqlOperator> bqlOperators;

    public LogicalPlanner(String query) {
        this.query = query;
    }

    private int traverse(LogicalNode root, BqlEqOperator operator, int index) {
        for (int i = index; i < bqlOperators.size(); i++) {
            BqlOperator bqlOperator = bqlOperators.get(i);
            if (bqlOperator.getLevel() <= operator.getLevel()) {
                return i;
            }
            switch (bqlOperator.getOperatorType()) {
                case EQ, LT, GT, GTE, NE, LTE:
                    if (bqlOperator.getValues() == null) {
                        return traverse(root, operator, i + 1);
                    }
                    LogicalComparisonFilter filter = new LogicalComparisonFilter(bqlOperator.getOperatorType());
                    filter.setField(operator.getField());
                    bqlOperator.getValues().forEach(filter::addBqlValue);
                    root.addFilter(filter);
                    break;
                case EXISTS:
                    boolean value = (boolean) bqlOperator.getValues().getFirst().value();
                    LogicalExistsFilter existsFilter = new LogicalExistsFilter(value);
                    existsFilter.setField(operator.getField());
                    root.addFilter(existsFilter);
                    break;
                case ALL:
                    LogicalAndFilter andOperator = new LogicalAndFilter();
                    bqlOperator.getValues().forEach(bqlValue -> {
                        LogicalComparisonFilter eqFilter = new LogicalComparisonFilter(OperatorType.EQ);
                        eqFilter.setField(operator.getField());
                        eqFilter.addBqlValue(bqlValue);
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
        switch (operator.getOperatorType()) {
            case EQ:
                BqlEqOperator eq = (BqlEqOperator) operator;
                if (eq.getValues() == null) {
                    return traverse(root, eq, idx + 1);
                } else {
                    LogicalComparisonFilter comparisonFilter = new LogicalComparisonFilter(OperatorType.EQ);
                    comparisonFilter.setField(eq.getField());
                    eq.getValues().forEach(comparisonFilter::addBqlValue);
                    root.addFilter(comparisonFilter);
                }
                break;
            case OR, AND:
                LogicalNode newRoot = makeRootNode(operator.getOperatorType());
                idx = traverse1(newRoot, operator.getLevel(), idx + 1);
                root.addFilter(newRoot);
                return idx;
        }
        return idx + 1;
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

        // Validate the parsed BQL query
        BqlValidator.validate(bqlOperators);

        // Run the normalizer to prune unnecessary operators and to apply custom rules.
        BqlNormalizer.normalize(bqlOperators);

        LogicalFullScan logicalScan = new LogicalFullScan();
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