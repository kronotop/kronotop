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
 * Predicate for range scan operations.
 * Uses Operand for type-safe bound representation supporting both literal values and parameter references.
 * Bounds can be null for unbounded ranges.
 */
public record RangeScanPredicate(String selector, Operand lowerBound, Operand upperBound, boolean includeLower,
                                 boolean includeUpper) {

    /**
     * Resolves the lower bound to a BqlValue using the parameter list.
     *
     * @param parameters the parameter list for resolving Param operands
     * @return the resolved lower bound value, or null if unbounded
     */
    public BqlValue resolveLowerBound(List<BqlValue> parameters) {
        return lowerBound != null ? lowerBound.resolve(parameters) : null;
    }

    /**
     * Resolves the upper bound to a BqlValue using the parameter list.
     *
     * @param parameters the parameter list for resolving Param operands
     * @return the resolved upper bound value, or null if unbounded
     */
    public BqlValue resolveUpperBound(List<BqlValue> parameters) {
        return upperBound != null ? upperBound.resolve(parameters) : null;
    }
}
