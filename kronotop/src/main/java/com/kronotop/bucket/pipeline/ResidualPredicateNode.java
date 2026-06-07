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

import com.kronotop.bucket.CollatorCache;
import com.kronotop.bucket.bql.ast.BqlValue;

import java.util.List;

/**
 * Interface for residual predicates that filter documents after index retrieval.
 * Supports parameterized queries through the parameters list.
 */
public interface ResidualPredicateNode {

    /**
     * Tests if the document satisfies this predicate.
     *
     * @param view          the document view containing virtual fields and content
     * @param parameters    the parameter list for resolving Param operands
     * @param collatorCache cache for acquiring collators, or null for binary comparison
     * @return true if the document satisfies the predicate
     */
    boolean test(DocumentView view, List<BqlValue> parameters, CollatorCache collatorCache);

    default boolean test(DocumentView view, List<BqlValue> parameters) {
        return test(view, parameters, null);
    }
}
