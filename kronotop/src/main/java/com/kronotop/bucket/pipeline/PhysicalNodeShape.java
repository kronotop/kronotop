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

import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.*;
import com.kronotop.internal.shapehash.BaseShapeHash;

import java.util.Arrays;
import java.util.List;

/**
 * Computes a shape hash for physical nodes using FNV-1a algorithm.
 * Mirrors the QueryShape class for BQL expressions.
 *
 * <p>The shape captures structural characteristics without actual parameter values,
 * enabling deterministic parameter extraction order that matches ParameterExtractor.</p>
 */
public final class PhysicalNodeShape extends BaseShapeHash {

    // Node type constants for distinguishing physical node types
    private static final int NODE_INDEX_SCAN = 101;
    private static final int NODE_FULL_SCAN = 102;
    private static final int NODE_RANGE_SCAN = 103;
    private static final int NODE_INDEX_INTERSECTION = 104;
    private static final int NODE_TRUE = 105;
    private static final int NODE_FALSE = 106;
    private static final int NODE_COMPOUND_INDEX_SCAN = 107;

    private PhysicalNodeShape() {
    }

    /**
     * Computes a 64-bit shape hash for a physical node.
     *
     * @param node the physical node to compute the shape for
     * @return a 64-bit FNV-1a hash representing the node shape
     */
    public static long compute(PhysicalNode node) {
        return computeNode(node, FNV_OFFSET_BASIS);
    }

    /**
     * Computes a hash for parameter binding purposes that matches QueryShape's ordering.
     * This hash skips wrapper node type prefixes (IndexScan, FullScan) to ensure the
     * relative ordering matches ParameterExtractor's ordering.
     *
     * @param node the physical node to compute the binding hash for
     * @return a 64-bit hash for parameter binding ordering
     */
    public static long computeForBinding(PhysicalNode node) {
        return computeNodeForBinding(node, FNV_OFFSET_BASIS);
    }

    private static long computeNodeForBinding(PhysicalNode node, long hash) {
        return switch (node) {
            case PhysicalFilter filter -> hashFilter(hash, filter);
            // Skip wrapper prefixes to match QueryShape ordering
            case PhysicalIndexScan indexScan -> computeNodeForBinding(indexScan.node(), hash);
            case PhysicalFullScan fullScan -> computeNodeForBinding(fullScan.node(), hash);
            case PhysicalRangeScan rangeScan -> hashRangeScanForBinding(hash, rangeScan);
            case PhysicalAnd and -> hashLogicalForBinding(hash, OP_AND, and.children());
            case PhysicalOr or -> hashLogicalForBinding(hash, OP_OR, or.children());
            case PhysicalNot not -> {
                long h = mix(hash, OP_NOT);
                yield computeNodeForBinding(not.child(), h);
            }
            case PhysicalElemMatch elemMatch -> {
                long h = mix(hash, OP_ELEMMATCH);
                h = mixString(h, elemMatch.selector());
                yield computeNodeForBinding(elemMatch.subPlan(), h);
            }
            case PhysicalIndexIntersection intersection -> hashIndexIntersectionForBinding(hash, intersection);
            case PhysicalCompoundIndexScan compoundScan -> hashCompoundIndexScanForBinding(hash, compoundScan);
            case PhysicalTrue ignored -> mix(hash, NODE_TRUE);
            case PhysicalFalse ignored -> mix(hash, NODE_FALSE);
        };
    }

    private static long computeNode(PhysicalNode node, long hash) {
        return switch (node) {
            case PhysicalFilter filter -> hashFilter(hash, filter);
            case PhysicalIndexScan indexScan -> {
                long h = mix(hash, NODE_INDEX_SCAN);
                yield computeNode(indexScan.node(), h);
            }
            case PhysicalFullScan fullScan -> {
                long h = mix(hash, NODE_FULL_SCAN);
                yield computeNode(fullScan.node(), h);
            }
            case PhysicalRangeScan rangeScan -> hashRangeScan(hash, rangeScan);
            case PhysicalAnd and -> hashLogical(hash, OP_AND, and.children());
            case PhysicalOr or -> hashLogical(hash, OP_OR, or.children());
            case PhysicalNot not -> {
                long h = mix(hash, OP_NOT);
                yield computeNode(not.child(), h);
            }
            case PhysicalElemMatch elemMatch -> {
                long h = mix(hash, OP_ELEMMATCH);
                h = mixString(h, elemMatch.selector());
                yield computeNode(elemMatch.subPlan(), h);
            }
            case PhysicalIndexIntersection intersection -> hashIndexIntersection(hash, intersection);
            case PhysicalCompoundIndexScan compoundScan -> hashCompoundIndexScan(hash, compoundScan);
            case PhysicalTrue ignored -> mix(hash, NODE_TRUE);
            case PhysicalFalse ignored -> mix(hash, NODE_FALSE);
        };
    }

    private static long hashFilter(long hash, PhysicalFilter filter) {
        // IMPORTANT: Hash computation order must match QueryShape to preserve
        // relative ordering for parameter binding alignment.
        long h = mix(hash, operatorCode(filter.op()));
        h = mixString(h, filter.selector());

        // For array operators ($in/$nin/$all), match QueryShape.hashArrayOp order:
        // op -> selector -> size -> sorted types
        if (filter.operand() instanceof List<?> list) {
            return mixListTypes(h, list);
        } else {
            // For single-value operators, match QueryShape.hashComparison order:
            // op -> selector -> valueType
            return mix(h, valueType(filter.operand()));
        }
    }

    private static long hashRangeScan(long hash, PhysicalRangeScan rangeScan) {
        long h = mix(hash, NODE_RANGE_SCAN);
        h = mixString(h, rangeScan.selector());

        // Include bound presence and types in shape
        if (rangeScan.lowerBound() != null) {
            h = mix(h, OP_GTE);  // or OP_GT depending on includeLower, but type matters more
            h = mix(h, valueType(rangeScan.lowerBound()));
            h = mix(h, rangeScan.includeLower() ? 1 : 0);
        }
        if (rangeScan.upperBound() != null) {
            h = mix(h, OP_LTE);  // or OP_LT depending on includeUpper
            h = mix(h, valueType(rangeScan.upperBound()));
            h = mix(h, rangeScan.includeUpper() ? 1 : 0);
        }

        return h;
    }

    private static long hashRangeScanForBinding(long hash, PhysicalRangeScan rangeScan) {
        // Skip the NODE_RANGE_SCAN prefix to match QueryShape ordering
        long h = mixString(hash, rangeScan.selector());

        if (rangeScan.lowerBound() != null) {
            h = mix(h, rangeScan.includeLower() ? OP_GTE : OP_GT);
            h = mix(h, valueType(rangeScan.lowerBound()));
        }
        if (rangeScan.upperBound() != null) {
            h = mix(h, rangeScan.includeUpper() ? OP_LTE : OP_LT);
            h = mix(h, valueType(rangeScan.upperBound()));
        }

        return h;
    }

    private static long hashSortedFilters(long hash, int nodeType, List<? extends PhysicalNode> filters, boolean forBinding) {
        long h = mix(hash, nodeType);
        h = mix(h, filters.size());

        long[] filterHashes = new long[filters.size()];
        for (int i = 0; i < filters.size(); i++) {
            filterHashes[i] = forBinding
                    ? computeNodeForBinding(filters.get(i), FNV_OFFSET_BASIS)
                    : computeNode(filters.get(i), FNV_OFFSET_BASIS);
        }
        Arrays.sort(filterHashes);
        for (long filterHash : filterHashes) {
            h = mix(h, filterHash);
        }

        return h;
    }

    private static long hashIndexIntersection(long hash, PhysicalIndexIntersection intersection) {
        return hashSortedFilters(hash, NODE_INDEX_INTERSECTION, intersection.filters(), false);
    }

    private static long hashIndexIntersectionForBinding(long hash, PhysicalIndexIntersection intersection) {
        return hashSortedFilters(hash, OP_AND, intersection.filters(), true);
    }

    private static long hashCompoundIndexScan(long hash, PhysicalCompoundIndexScan compoundScan) {
        return hashSortedFilters(hash, NODE_COMPOUND_INDEX_SCAN, compoundScan.filters(), false);
    }

    private static long hashCompoundIndexScanForBinding(long hash, PhysicalCompoundIndexScan compoundScan) {
        return hashSortedFilters(hash, OP_AND, compoundScan.filters(), true);
    }

    private static long hashLogical(long hash, int op, List<PhysicalNode> children) {
        long h = mix(hash, op);
        h = mix(h, children.size());

        // Compute child hashes, sort for canonical order, then combine
        long[] childHashes = new long[children.size()];
        for (int i = 0; i < children.size(); i++) {
            childHashes[i] = computeNode(children.get(i), FNV_OFFSET_BASIS);
        }
        Arrays.sort(childHashes);
        for (long childHash : childHashes) {
            h = mix(h, childHash);
        }

        return h;
    }

    private static long hashLogicalForBinding(long hash, int op, List<PhysicalNode> children) {
        long h = mix(hash, op);
        h = mix(h, children.size());

        // Use computeNodeForBinding for consistent ordering with ParameterExtractor
        long[] childHashes = new long[children.size()];
        for (int i = 0; i < children.size(); i++) {
            childHashes[i] = computeNodeForBinding(children.get(i), FNV_OFFSET_BASIS);
        }
        Arrays.sort(childHashes);
        for (long childHash : childHashes) {
            h = mix(h, childHash);
        }

        return h;
    }

    private static int operatorCode(Operator op) {
        return switch (op) {
            case EQ -> OP_EQ;
            case NE -> OP_NE;
            case GT -> OP_GT;
            case GTE -> OP_GTE;
            case LT -> OP_LT;
            case LTE -> OP_LTE;
            case IN -> OP_IN;
            case NIN -> OP_NIN;
            case ALL -> OP_ALL;
            case SIZE -> OP_SIZE;
            case EXISTS -> OP_EXISTS;
            case REGEX -> OP_REGEX;
        };
    }
}
