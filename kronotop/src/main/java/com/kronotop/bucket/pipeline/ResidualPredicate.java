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

import com.ibm.icu.text.Collator;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.CollatorCache;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.planner.Operator;

import java.util.List;

/**
 * Predicate for residual (post-retrieval) filtering.
 * Uses Operand for type-safe operand representation supporting both literal values and parameter references.
 */
public record ResidualPredicate(int id, String selector, Operator op,
                                Operand operand, Collation collation) implements ResidualPredicateNode {

    public ResidualPredicate(int id, String selector, Operator op, Operand operand) {
        this(id, selector, op, operand, null);
    }

    /**
     * Tests if the given document satisfies this predicate.
     *
     * @param view          the document view containing virtual fields and content
     * @param parameters    the parameter list for resolving Param operands
     * @param collatorCache cache for acquiring collators, or null for binary comparison
     * @return true if the document satisfies the predicate
     */
    public boolean test(DocumentView view, List<BqlValue> parameters, CollatorCache collatorCache) {
        try {
            Collator collator = (collation != null && collatorCache != null)
                    ? collatorCache.acquire(collation) : null;
            return PredicateEvaluator.testResidualPredicate(this, view, parameters, collator);
        } finally {
            view.getContent().rewind();
        }
    }
}
