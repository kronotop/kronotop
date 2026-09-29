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
 * Computes a 64-bit query shape hash for plan caching.
 *
 * <p>The shape includes operators, field selectors, value types, the AND/OR/NOT/$elemMatch nesting,
 * the number of AND/OR children, the array sizes of $in/$nin/$all and the boolean value of $exists.
 * Other values are not included. The order of children in $and/$or and the order of values in
 * $in/$nin/$all do not change the shape.</p>
 *
 * <p>The expression is hashed with FNV-1a. The SORTBY field and the collation, when given, are
 * added to that hash.</p>
 */
public final class QueryShape extends BaseShapeHash {

    private QueryShape() {
    }

    /**
     * Computes a 64-bit shape hash for a BQL expression.
     *
     * @param expr the BQL expression to compute the shape for
     * @return the shape hash
     */
    public static long compute(BqlExpr expr) {
        return computeExpr(expr, FNV_OFFSET_BASIS);
    }

    /**
     * Computes a 64-bit shape hash that includes the SORTBY field.
     * Queries with different sort fields may produce different plans and must have distinct cache keys.
     *
     * @param expr        the BQL expression to compute the shape for
     * @param sortByField the SORTBY field name, or null if no sorting is requested
     * @return a 64-bit hash representing the query shape including sort context
     */
    public static long compute(BqlExpr expr, String sortByField) {
        long shapeHash = compute(expr);
        if (sortByField != null) {
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
                h = mix(h, TYPE_INT32);
                yield h;
            }

            case BqlExists(String selector, boolean exists) -> {
                long h = mix(hash, OP_EXISTS);
                h = mixString(h, selector);
                h = mix(h, exists ? 1 : 0);
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
