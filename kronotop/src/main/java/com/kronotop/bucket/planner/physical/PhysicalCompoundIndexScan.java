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

package com.kronotop.bucket.planner.physical;

import com.kronotop.bucket.index.CompoundIndexDefinition;

import java.util.List;
import java.util.Objects;

/**
 * Physical node representing a compound index scan that uses a multi-field index
 * to efficiently satisfy multiple filter conditions in a single scan operation.
 *
 * @param id      unique node identifier (excluded from equals/hashCode)
 * @param index   the compound index definition to scan
 * @param filters ordered list of filters matching the compound index field order
 */
public record PhysicalCompoundIndexScan(
        int id,
        CompoundIndexDefinition index,
        List<PhysicalFilter> filters
) implements PhysicalNode {

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof PhysicalCompoundIndexScan other)) return false;
        return Objects.equals(index, other.index) &&
                Objects.equals(filters, other.filters);
        // Note: id is intentionally excluded from comparison
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, filters);
        // Note: id is intentionally excluded from hash
    }
}
