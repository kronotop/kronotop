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

/*
 * Potential index usage with $not operator:
 *
 * $not can use an index only when it negates a single, simple,
 * indexable predicate on a scalar (non-array) field.
 *
 * Index usage is possible ONLY when:
 *   - The indexed field is NOT an array (no multi-key semantics)
 *   - $not wraps exactly one comparison operator
 *     ($eq, $gt, $gte, $lt, $lte)
 *   - The predicate defines a finite, well-bounded value domain
 *
 * Example cases where an index MAY be used:
 *   { age: { $not: { $eq: 30 } } }
 *   { score: { $not: { $gt: 100 } } }
 *
 * Even in these cases, execution typically requires:
 *   INDEX_SCAN followed by document-level FILTERING
 * (not a pure index-only scan).
 *
 * Index is NOT usable when:
 *   - $not is applied to arrays or multi-key indexed fields
 *   - $not wraps $regex, $exists, $elemMatch, or any logical operator
 *   - The negation produces a large or unbounded negative result set
 *
 * Practical guideline:
 *   $not is semantically correct but not index-friendly.
 *   Prefer rewriting queries using positive predicates when possible.
 */

/**
 * Residual predicate node that negates the result of its child predicate.
 */
public class ResidualNotNode implements ResidualPredicateNode {
    private final ResidualPredicateNode child;

    public ResidualNotNode(ResidualPredicateNode child) {
        this.child = child;
    }

    public ResidualPredicateNode child() {
        return child;
    }

    @Override
    public boolean test(DocumentView view, List<BqlValue> parameters, CollatorCache collatorCache) {
        return !child.test(view, parameters, collatorCache);
    }
}
