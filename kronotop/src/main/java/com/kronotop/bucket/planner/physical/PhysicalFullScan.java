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

import java.util.Objects;

public record PhysicalFullScan(int id, PhysicalNode node) implements PhysicalNode {
    @Override
    public <R> R accept(PhysicalPlanVisitor<R> visitor) {
        return visitor.visitFullScan(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof PhysicalFullScan other)) return false;
        return Objects.equals(node, other.node);
        // Note: id is intentionally excluded from comparison
    }

    @Override
    public int hashCode() {
        return Objects.hash(node);
        // Note: id is intentionally excluded from hash
    }
}
