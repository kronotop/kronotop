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

package com.kronotop.bucket.bql;

import com.kronotop.bucket.Collation;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.internal.shapehash.BaseShapeHash;

import java.util.Arrays;
import java.util.List;

/**
 * Computes a query shape hash for plan caching using FNV-1a algorithm.
 *
 * <p>A query shape captures the structural characteristics of a query without
 * the actual parameter values. Two queries with the same shape will have
 * identical execution plans, making the shape suitable as a cache key.</p>
 *
 * <p>The shape includes:</p>
 * <ul>
 *   <li>Operator types ($eq, $gt, $in, $and, etc.)</li>
 *   <li>Field selectors (field names being queried)</li>
 *   <li>Value types (INT32, STRING, etc.) but not actual values</li>
 *   <li>Structural nesting (AND/OR/NOT hierarchy)</li>
 *   <li>Array sizes for $in/$nin/$all operators</li>
 * </ul>
 *
 * <p>Order independence:</p>
 * <ul>
 *   <li>Field ordering within $and/$or does not affect the shape hash</li>
 *   <li>Value ordering within $in/$nin/$all does not affect the shape hash</li>
 * </ul>
 *
 * <p>For $in/$nin/$all operators, all value types are hashed (not just the first),
 * so mixed-type arrays produce different shapes than homogeneous arrays.</p>
 */
public final class QueryShape extends BaseShapeHash {

    private QueryShape() {
    }

    /**
     * Computes a 64-bit shape hash for a BQL expression.
     *
     * <p>Queries with the same shape produce identical hashes and can share
     * cached execution plans. The hash is computed using FNV-1a algorithm
     * with minimal allocations.</p>
     *
     * @param expr the BQL expression to compute the shape for
     * @return a 64-bit FNV-1a hash representing the query shape
     */
    public static long compute(BqlExpr expr) {
        return computeExpr(expr, FNV_OFFSET_BASIS);
    }

    /**
     * Computes a 64-bit shape hash that includes the SORTBY field.
     * Queries with different sort fields produce different plans and must have distinct cache keys.
     *
     * @param expr        the BQL expression to compute the shape for
     * @param sortByField the SORTBY field name, or null if no sorting is requested
     * @return a 64-bit hash representing the query shape including sort context
     */
    public static long compute(BqlExpr expr, String sortByField) {
        long shapeHash = compute(expr);
        if (sortByField != null) {
            // Include sortByField in the cache key - different sort fields produce different plans
            return 31 * shapeHash + sortByField.hashCode();
        }
        return shapeHash;
    }

    /**
     * Computes a 64-bit shape hash that includes the SORTBY field and per-query collation.
     * Queries with different collations may produce different plans and must have distinct cache keys.
     *
     * @param expr        the BQL expression to compute the shape for
     * @param sortByField the SORTBY field name, or null if no sorting is requested
     * @param collation   the per-query collation, or null if not specified
     * @return a 64-bit hash representing the query shape including sort and collation context
     */
    public static long compute(BqlExpr expr, String sortByField, Collation collation) {
        long shapeHash = compute(expr, sortByField);
        if (collation != null) {
            return 31 * shapeHash + collation.hashCode();
        }
        return shapeHash;
    }

    private static long computeExpr(BqlExpr expr, long hash) {
        return switch (expr) {
            case BqlEq(String selector, BqlValue value) -> hashComparison(hash, OP_EQ, selector, value);

            case BqlNe(String selector, BqlValue value) -> hashComparison(hash, OP_NE, selector, value);

            case BqlGt(String selector, BqlValue value) -> hashComparison(hash, OP_GT, selector, value);

            case BqlGte(String selector, BqlValue value) -> hashComparison(hash, OP_GTE, selector, value);

            case BqlLt(String selector, BqlValue value) -> hashComparison(hash, OP_LT, selector, value);

            case BqlLte(String selector, BqlValue value) -> hashComparison(hash, OP_LTE, selector, value);

            case BqlIn(String selector, List<BqlValue> values) -> hashArrayOp(hash, OP_IN, selector, values);

            case BqlNin(String selector, List<BqlValue> values) -> hashArrayOp(hash, OP_NIN, selector, values);

            case BqlAll(String selector, List<BqlValue> values) -> hashArrayOp(hash, OP_ALL, selector, values);

            case BqlSize(String selector, int ignored) -> {
                long h = mix(hash, OP_SIZE);
                h = mixString(h, selector);
                h = mix(h, TYPE_INT32); // size is always int
                yield h;
            }

            case BqlExists(String selector, boolean exists) -> {
                long h = mix(hash, OP_EXISTS);
                h = mixString(h, selector);
                h = mix(h, exists ? 1 : 0); // include the boolean value in shape
                yield h;
            }

            case BqlRegex(String selector, RegexVal value) -> hashComparison(hash, OP_REGEX, selector, value);

            case BqlAnd(List<BqlExpr> children) -> hashLogical(hash, OP_AND, children);

            case BqlOr(List<BqlExpr> children) -> hashLogical(hash, OP_OR, children);

            case BqlNot(BqlExpr child) -> {
                long h = mix(hash, OP_NOT);
                yield computeExpr(child, h);
            }

            case BqlElemMatch(String selector, BqlExpr child) -> {
                long h = mix(hash, OP_ELEMMATCH);
                h = mixString(h, selector);
                yield computeExpr(child, h);
            }
        };
    }

    private static long hashComparison(long hash, int op, String selector, BqlValue value) {
        long h = mix(hash, op);
        h = mixString(h, selector);
        h = mix(h, valueType(value));
        return h;
    }

    private static long hashArrayOp(long hash, int op, String selector, List<BqlValue> values) {
        long h = mix(hash, op);
        h = mixString(h, selector);
        return mixListTypes(h, values);
    }

    private static long hashLogical(long hash, int op, List<BqlExpr> children) {
        long h = mix(hash, op);
        h = mix(h, children.size());
        // Compute child hashes, sort for canonical order, then combine
        long[] childHashes = new long[children.size()];
        for (int i = 0; i < children.size(); i++) {
            childHashes[i] = computeExpr(children.get(i), FNV_OFFSET_BASIS);
        }
        Arrays.sort(childHashes);
        for (long childHash : childHashes) {
            h = mix(h, childHash);
        }
        return h;
    }
}
