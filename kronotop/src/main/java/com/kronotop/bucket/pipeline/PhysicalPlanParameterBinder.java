/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.ShapeHashSorter;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Walks the physical plan in canonical order and builds a parameter binding map.
 * The canonical order matches ParameterExtractor's traversal order for BQL expressions.
 */
public final class PhysicalPlanParameterBinder {

    private PhysicalPlanParameterBinder() {
    }

    /**
     * Walks the physical plan in canonical order and builds a mapping from
     * physical node ID to parameter bindings.
     *
     * @param root       the root physical node
     * @param parameters the extracted parameter values from the original query
     * @return map from node ID to list of parameter bindings
     */
    public static Map<Integer, List<ParamBinding>> bind(PhysicalNode root, List<BqlValue> parameters) {
        Map<Integer, List<ParamBinding>> bindings = new HashMap<>();
        bindNode(root, bindings, parameters);
        return bindings;
    }

    private static void bindNode(PhysicalNode node, Map<Integer, List<ParamBinding>> bindings, List<BqlValue> parameters) {
        switch (node) {
            case PhysicalFilter filter -> {
                // EXISTS has no parameter (boolean is part of shape)
                if (filter.op() == Operator.EXISTS) {
                    return;
                }

                // For $in/$nin/$all, each value in the list gets a separate parameter
                if (filter.operand() instanceof List<?> list) {
                    List<ParamBinding> filterBindings = new ArrayList<>();
                    for (int i = 0; i < list.size(); i++) {
                        BqlValue value = toBqlValue(list.get(i));
                        int paramIndex = findParameterIndex(parameters, value);
                        filterBindings.add(new ParamBinding(paramIndex, i));
                    }
                    bindings.put(filter.id(), filterBindings);
                } else {
                    // Single operand - find its index in parameters
                    // SIZE stores Integer, so convert to Int32Val for lookup
                    BqlValue value = toBqlValue(filter.operand());
                    int paramIndex = findParameterIndex(parameters, value);
                    bindings.put(filter.id(), List.of(new ParamBinding(paramIndex, 0)));
                }
            }

            case PhysicalIndexScan indexScan -> // Delegate to the wrapped filter node
                    bindNode(indexScan.node(), bindings, parameters);

            case PhysicalFullScan fullScan -> // Delegate to the wrapped filter node
                    bindNode(fullScan.node(), bindings, parameters);

            case PhysicalRangeScan rangeScan -> {
                // Two operands: lower bound and upper bound (if present)
                List<ParamBinding> rangeBindings = new ArrayList<>();
                int occurrence = 0;
                if (rangeScan.lowerBound() != null) {
                    int paramIndex = findParameterIndex(parameters, (BqlValue) rangeScan.lowerBound());
                    rangeBindings.add(new ParamBinding(paramIndex, occurrence++));
                }
                if (rangeScan.upperBound() != null) {
                    int paramIndex = findParameterIndex(parameters, (BqlValue) rangeScan.upperBound());
                    rangeBindings.add(new ParamBinding(paramIndex, occurrence));
                }
                bindings.put(rangeScan.id(), rangeBindings);
            }

            case PhysicalAnd and -> // Sort children by shape hash (canonical order)
                    bindChildrenInCanonicalOrder(and.children(), bindings, parameters);

            case PhysicalOr or -> // Sort children by shape hash (canonical order)
                    bindChildrenInCanonicalOrder(or.children(), bindings, parameters);

            case PhysicalNot not -> // Recursively bind the child
                    bindNode(not.child(), bindings, parameters);

            case PhysicalElemMatch elemMatch -> // Recursively bind the sub-plan
                    bindNode(elemMatch.subPlan(), bindings, parameters);

            case PhysicalIndexIntersection intersection -> // Sort filters by shape hash and bind in canonical order
                    bindChildrenInCanonicalOrder(intersection.filters(), bindings, parameters);

            case PhysicalCompoundIndexScan compoundScan -> // Sort filters by shape hash and bind in canonical order
                    bindChildrenInCanonicalOrder(compoundScan.filters(), bindings, parameters);

            case PhysicalTrue ignored -> {
                // No parameters
            }

            case PhysicalFalse ignored -> {
                // No parameters
            }
        }
    }

    /**
     * Converts an operand to a BqlValue.
     */
    private static BqlValue toBqlValue(Object operand) {
        if (operand instanceof BqlValue bqlValue) {
            return bqlValue;
        }
        return BSONUtil.convertObjectToBqlValue(operand);
    }

    /**
     * Finds the index of a value in the parameters list.
     *
     * @param parameters the list of extracted parameters
     * @param value      the value to find
     * @return the index of the value in the parameters list
     * @throws IllegalStateException if the value is not found
     */
    private static int findParameterIndex(List<BqlValue> parameters, BqlValue value) {
        for (int i = 0; i < parameters.size(); i++) {
            if (parameters.get(i).equals(value)) {
                return i;
            }
        }
        throw new IllegalStateException("Parameter not found: " + value);
    }

    /**
     * Binds children in canonical order by sorting them by shape hash.
     * Uses stable insertion sort that preserves order for identical-shape siblings.
     * Uses computeForBinding() to match ParameterExtractor's ordering.
     */
    private static void bindChildrenInCanonicalOrder(
            List<? extends PhysicalNode> children,
            Map<Integer, List<ParamBinding>> bindings,
            List<BqlValue> parameters) {
        int size = children.size();
        if (size == 0) {
            return;
        }

        // Compute shape hashes using computeForBinding to match ParameterExtractor ordering
        long[] hashes = new long[size];
        int[] indices = new int[size];
        for (int i = 0; i < size; i++) {
            hashes[i] = PhysicalNodeShape.computeForBinding(children.get(i));
            indices[i] = i;
        }

        ShapeHashSorter.sortIndicesByHash(hashes, indices);

        // Bind children in sorted order
        for (int i = 0; i < size; i++) {
            bindNode(children.get(indices[i]), bindings, parameters);
        }
    }

    /**
     * Represents a parameter binding for a physical node.
     *
     * @param paramIndex the index into the parameter list
     * @param occurrence distinguishes multiple operands in the same node (e.g., lower/upper bounds)
     */
    public record ParamBinding(int paramIndex, int occurrence) {
    }
}
