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

import com.kronotop.bucket.bql.ast.BqlValue;

import java.util.List;

/**
 * Type-safe operand representation for pipeline predicates.
 * Enables plan caching with parameterized execution by deferring value resolution.
 */
public sealed interface Operand permits Operand.Literal, Operand.Param, Operand.LiteralList, Operand.ParamList {

    /**
     * Resolves this operand to a BqlValue using the provided parameter list.
     *
     * @param parameters the parameter list for resolving Param operands
     * @return the resolved BqlValue
     */
    default BqlValue resolve(List<BqlValue> parameters) {
        return switch (this) {
            case Literal(BqlValue v) -> v;
            case Param(ParamRef ref) -> parameters.get(ref.index());
            case LiteralList ignored ->
                    throw new IllegalStateException("Cannot resolve LiteralList to a single BqlValue");
            case ParamList ignored -> throw new IllegalStateException("Cannot resolve ParamList to a single BqlValue");
        };
    }

    /**
     * Resolves this operand to its underlying value.
     *
     * @param parameters the parameter list for resolving Param operands
     * @return the resolved value (BqlValue for single values, List for list operands)
     */
    default Object resolveAny(List<BqlValue> parameters) {
        return switch (this) {
            case Literal(BqlValue v) -> v;
            case Param(ParamRef ref) -> parameters.get(ref.index());
            case LiteralList(List<BqlValue> values) -> values;
            case ParamList(List<ParamRef> refs) -> refs.stream().map(ref -> parameters.get(ref.index())).toList();
        };
    }

    /**
     * A literal operand containing a concrete BqlValue.
     */
    record Literal(BqlValue value) implements Operand {
    }

    /**
     * A parameter reference operand that will be resolved at execution time.
     */
    record Param(ParamRef ref) implements Operand {
    }

    /**
     * A literal list operand containing concrete BqlValues (for $in/$nin/$all operators).
     */
    record LiteralList(List<BqlValue> values) implements Operand {
    }

    /**
     * A parameter list operand for $in/$nin/$all operators in parameterized queries.
     * Each ParamRef references a parameter that will be resolved at execution time.
     */
    record ParamList(List<ParamRef> refs) implements Operand {
    }
}
