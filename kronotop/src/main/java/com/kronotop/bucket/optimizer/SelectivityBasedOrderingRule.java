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

package com.kronotop.bucket.optimizer;

import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Selectivity-Based Ordering Rule that reorders conditions in AND/OR operations
 * based on their estimated selectivity to optimize execution performance.
 * <p>
 * This rule performs statistics-free ordering of boolean predicates (AND / OR) to optimize execution efficiency via
 * short-circuiting. It does not change the access path or estimate absolute cardinality; instead, it applies lightweight
 * heuristics (operator type, index availability, and node kind) to determine the most efficient evaluation order of
 * physical nodes.
 * <p>
 * While histogram-based optimization can provide finer-grained costs, this rule ensures a performant baseline ordering
 * at the physical plan level, even when statistics are unavailable or stale.
 *
 * <p>
 * This rule implements execution order optimization by:
 * <ul>
 * <li>For AND operations: Orders conditions from most selective (lowest cost) to least selective</li>
 * <li>For OR operations: Orders conditions from least selective to most selective for better short-circuiting</li>
 * <li>Considers index availability, operator type, and field characteristics for selectivity estimation</li>
 * </ul>
 * <p>
 * Selectivity estimation considers:
 * <ul>
 * <li>Index availability (indexed conditions are generally more selective)</li>
 * <li>Operator type (equality is more selective than range operations)</li>
 * <li>Node type (PhysicalIndexScan > PhysicalRangeScan > PhysicalFullScan)</li>
 * </ul>
 */
public class SelectivityBasedOrderingRule implements PhysicalOptimizationRule {

    @Override
    public PhysicalNode apply(PhysicalNode node, BucketMetadata metadata, PlannerContext context) {
        SelectivityEstimator estimator = new SelectivityEstimator(metadata);

        return switch (node) {
            case PhysicalAnd and -> optimizeAnd(and, estimator, metadata, context);
            case PhysicalOr or -> optimizeOr(or, estimator, metadata, context);
            case PhysicalNot not -> new PhysicalNot(context.nextId(), apply(not.child(), metadata, context));
            case PhysicalElemMatch elemMatch -> new PhysicalElemMatch(
                    context.nextId(),
                    elemMatch.selector(),
                    apply(elemMatch.subPlan(), metadata, context)
            );
            default -> node; // No optimization for other nodes
        };
    }

    /**
     * Optimize AND operations by ordering conditions from most to least selective.
     * This allows early termination when a highly selective condition fails.
     */
    private PhysicalNode optimizeAnd(PhysicalAnd and, SelectivityEstimator estimator, BucketMetadata metadata, PlannerContext context) {
        List<PhysicalNode> optimizedChildren = new ArrayList<>();

        // First, recursively optimize children
        for (PhysicalNode child : and.children()) {
            optimizedChildren.add(apply(child, metadata, context));
        }

        // Sort children by selectivity (most selective first for AND)
        optimizedChildren.sort(Comparator.comparingDouble(estimator::estimateSelectivity));

        // Return simplified structure
        if (optimizedChildren.size() == 1) {
            return optimizedChildren.get(0);
        } else if (!optimizedChildren.equals(and.children())) {
            // Only create new node if order changed or children were optimized
            return new PhysicalAnd(context.nextId(), optimizedChildren);
        } else {
            return and;
        }
    }

    /**
     * Optimize OR operations by ordering conditions from least to most selective.
     * This allows better short-circuiting when a less selective condition succeeds early.
     */
    private PhysicalNode optimizeOr(PhysicalOr or, SelectivityEstimator estimator, BucketMetadata metadata, PlannerContext context) {
        List<PhysicalNode> optimizedChildren = new ArrayList<>();

        // First, recursively optimize children
        for (PhysicalNode child : or.children()) {
            optimizedChildren.add(apply(child, metadata, context));
        }

        // Sort children by selectivity (least selective first for OR)
        optimizedChildren.sort(Comparator.comparingDouble(estimator::estimateSelectivity).reversed());

        // Return simplified structure
        if (optimizedChildren.size() == 1) {
            return optimizedChildren.get(0);
        } else if (!optimizedChildren.equals(or.children())) {
            // Only create new node if order changed or children were optimized
            return new PhysicalOr(context.nextId(), optimizedChildren);
        } else {
            return or;
        }
    }

    @Override
    public String getName() {
        return "SelectivityBasedOrdering";
    }

    @Override
    public int getPriority() {
        return 60; // Medium priority - after index optimizations, before lower-level optimizations
    }

    @Override
    public boolean canApply(PhysicalNode node) {
        return switch (node) {
            case PhysicalAnd and -> and.children().size() > 1;
            case PhysicalOr or -> or.children().size() > 1;
            case PhysicalNot not -> true;
            case PhysicalElemMatch elemMatch -> true;
            default -> false;
        };
    }

    /**
     * Estimates the selectivity cost of a physical node.
     * Lower values indicate higher selectivity (better performance).
     */
    private record SelectivityEstimator(BucketMetadata metadata) {

        /**
         * Estimate the selectivity cost of a physical node.
         * Lower cost means higher selectivity (fewer expected results).
         *
         * @param node the physical node to estimate
         * @return estimated cost (lower = more selective)
         */
        public double estimateSelectivity(PhysicalNode node) {
            return switch (node) {
                case PhysicalIndexScan indexScan -> estimateIndexScanSelectivity(indexScan);
                case PhysicalRangeScan rangeScan -> estimateRangeScanSelectivity(rangeScan);
                case PhysicalIndexIntersection intersection -> estimateIntersectionSelectivity(intersection);
                case PhysicalFullScan fullScan -> estimateFullScanSelectivity(fullScan);
                case PhysicalFilter filter -> estimateFilterSelectivity(filter);
                case PhysicalAnd and -> estimateAndSelectivity(and);
                case PhysicalOr or -> estimateOrSelectivity(or);
                case PhysicalNot not -> estimateNotSelectivity(not);
                case PhysicalElemMatch elemMatch -> estimateElemMatchSelectivity(elemMatch);
                case PhysicalTrue ignored -> 0.0; // Always true, very low selectivity
                case PhysicalFalse ignored -> 1.0; // Always false, highest selectivity
            };
        }

        private double estimateIndexScanSelectivity(PhysicalIndexScan indexScan) {
            if (!(indexScan.node() instanceof PhysicalFilter filter)) {
                return 20.0; // Default for non-filter index scan
            }

            // Base selectivity for indexed operations is good
            double baseCost = 10.0;

            // Adjust based on operator type
            return baseCost + getOperatorSelectivityAdjustment(filter.op());
        }

        private double estimateRangeScanSelectivity(PhysicalRangeScan rangeScan) {
            // Range scans are generally less selective than equality index scans
            // but better than full scans since they use indexes
            return 15.0;
        }

        private double estimateIntersectionSelectivity(PhysicalIndexIntersection intersection) {
            // Index intersections are typically very selective since they
            // combine multiple indexed conditions
            return 5.0 + (intersection.indexes().size() * 2.0);
        }

        private double estimateFullScanSelectivity(PhysicalFullScan fullScan) {
            if (!(fullScan.node() instanceof PhysicalFilter filter)) {
                return 100.0; // Full scan without filter is worst case
            }

            // Full scans are expensive, but operator type still matters
            double baseCost = 80.0;
            return baseCost + getOperatorSelectivityAdjustment(filter.op());
        }

        private double estimateFilterSelectivity(PhysicalFilter filter) {
            // Raw filters without index information - estimate based on operator
            double baseCost = 40.0;
            return baseCost + getOperatorSelectivityAdjustment(filter.op());
        }

        private double estimateAndSelectivity(PhysicalAnd and) {
            // AND operations combine selectivity - take the minimum (most selective)
            return and.children().stream()
                    .mapToDouble(this::estimateSelectivity)
                    .min()
                    .orElse(50.0);
        }

        private double estimateOrSelectivity(PhysicalOr or) {
            // OR operations reduce selectivity - take the maximum (least selective)
            return or.children().stream()
                    .mapToDouble(this::estimateSelectivity)
                    .max()
                    .orElse(50.0);
        }

        private double estimateNotSelectivity(PhysicalNot not) {
            // NOT operations invert selectivity
            double childSelectivity = estimateSelectivity(not.child());
            return 100.0 - childSelectivity;
        }

        private double estimateElemMatchSelectivity(PhysicalElemMatch elemMatch) {
            // Element match operations are moderately selective
            return 30.0 + estimateSelectivity(elemMatch.subPlan()) * 0.5;
        }

        /**
         * Get selectivity adjustment based on operator type.
         * Lower values are more selective.
         */
        private double getOperatorSelectivityAdjustment(Operator op) {
            return switch (op) {
                case EQ -> 0.0;    // Equality is most selective
                case NE -> 10.0;   // Not equal is least selective
                case LT, LTE, GT, GTE -> 5.0;  // Range operators are moderately selective
                case IN -> 2.0;    // IN is fairly selective
                case NIN -> 8.0;   // NOT IN is less selective
                case EXISTS -> 15.0; // EXISTS is not very selective
                case SIZE -> 10.0; // SIZE is moderately selective
                case ALL -> 6.0;   // ALL is moderately selective
            };
        }
    }
}