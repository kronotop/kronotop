/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.bucket.planner.physical;

import com.kronotop.bucket.index.IndexDefinition;

import java.util.List;
import java.util.Objects;

/**
 * Physical node representing an index intersection operation.
 * <p>
 * This optimized node combines multiple indexed conditions into a single
 * intersection scan that can efficiently find documents matching all conditions
 * by intersecting the results from multiple indexes.
 * <p>
 * Example: AND(name="john", age=25) → PhysicalIndexIntersection([name_index, age_index], [filters])
 */
public record PhysicalIndexIntersection(
        int id,
        List<IndexDefinition> indexes,
        List<PhysicalFilter> filters
) implements PhysicalNode {

    @Override
    public <R> R accept(PhysicalPlanVisitor<R> visitor) {
        return visitor.visitIndexIntersection(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof PhysicalIndexIntersection other)) return false;
        return Objects.equals(indexes, other.indexes) &&
                Objects.equals(filters, other.filters);
        // Note: id is intentionally excluded from comparison
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexes, filters);
        // Note: id is intentionally excluded from hash
    }
}