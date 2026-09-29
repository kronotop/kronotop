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

import com.kronotop.bucket.ShapeHashSorter;
import com.kronotop.bucket.bql.ast.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Extracts parameter values from BQL expressions in a deterministic order.
 *
 * <p>AND/OR children are sorted with {@link QueryShape#compute(BqlExpr)}, so queries with the same
 * shape hash fill the same parameter slots and a cached plan can be reused with new values.
 * Using {@code QueryShape.compute()} itself keeps the two classes from drifting apart.</p>
 *
 * <p>AND/OR children with identical shape hashes (same field, operator and value type) keep
 * their original order, so their parameters come out in query order:</p>
 *
 * <pre>{@code
 * // Same shape hash, but different parameter order:
 * {"$or": [{"brand": "Apple"}, {"brand": "Samsung"}]}  -> [Apple, Samsung]
 * {"$or": [{"brand": "Samsung"}, {"brand": "Apple"}]}  -> [Samsung, Apple]
 * }</pre>
 *
 * <p>This is correct because AND and OR are commutative. The parameters go into
 * structurally identical slots, so the result does not depend on their order.</p>
 */
public final class ParameterExtractor {

    private ParameterExtractor() {
    }

    /**
     * Extracts parameter values from a BQL expression.
     *
     * @param expr the BQL expression to extract parameters from
     * @return list of parameter values in canonical order
     */
    public static List<BqlValue> extract(BqlExpr expr) {
        List<BqlValue> output = new ArrayList<>();
        extractInto(expr, output);
        return output;
    }

    /**
     * Extracts parameter values into an existing list.
     *
     * @param expr   the BQL expression to extract parameters from
     * @param output the list to append parameters to
     */
    public static void extractInto(BqlExpr expr, List<BqlValue> output) {
        switch (expr) {
            case BqlEq(String ignored, BqlValue value) -> output.add(value);
            case BqlNe(String ignored, BqlValue value) -> output.add(value);
            case BqlGt(String ignored, BqlValue value) -> output.add(value);
            case BqlGte(String ignored, BqlValue value) -> output.add(value);
            case BqlLt(String ignored, BqlValue value) -> output.add(value);
            case BqlLte(String ignored, BqlValue value) -> output.add(value);
            case BqlIn(String ignored, List<BqlValue> values) -> output.addAll(values);
            case BqlNin(String ignored, List<BqlValue> values) -> output.addAll(values);
            case BqlAll(String ignored, List<BqlValue> values) -> output.addAll(values);
            case BqlSize(String ignored, int size) -> output.add(new Int32Val(size));
            case BqlRegex(String ignored, RegexVal value) -> output.add(value);
            case BqlExists(String ignored1, boolean ignored2) -> {
                // No parameters - boolean is part of the shape
            }
            case BqlAnd(List<BqlExpr> children) -> extractLogical(children, output);
            case BqlOr(List<BqlExpr> children) -> extractLogical(children, output);
            case BqlNot(BqlExpr child) -> extractInto(child, output);
            case BqlElemMatch(String ignored, BqlExpr child) -> extractInto(child, output);
        }
    }

    /**
     * Extracts parameters from AND/OR children in canonical order.
     * Uses QueryShape.compute() for sorting to ensure lockstep with shape hashing.
     */
    private static void extractLogical(List<BqlExpr> children, List<BqlValue> output) {
        int size = children.size();
        if (size == 0) {
            return;
        }

        // Compute shape hashes and build an index array
        long[] hashes = new long[size];
        int[] indices = new int[size];
        for (int i = 0; i < size; i++) {
            hashes[i] = QueryShape.compute(children.get(i));
            indices[i] = i;
        }

        ShapeHashSorter.sortIndicesByHash(hashes, indices);

        // Extract parameters in sorted order
        for (int i = 0; i < size; i++) {
            extractInto(children.get(indices[i]), output);
        }
    }
}
