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

import javax.annotation.Nonnull;

public record PhysicalFalse(int id) implements PhysicalNode {

    public static final PhysicalFalse INSTANCE = new PhysicalFalse(-1);

    @Override
    public <R> R accept(PhysicalPlanVisitor<R> visitor) {
        return visitor.visitFalse(this);
    }

    @Override
    public boolean equals(Object obj) {
        // All PhysicalFalse instances are equal regardless of id
        return obj instanceof PhysicalFalse;
    }
    
    @Override
    public int hashCode() {
        // All PhysicalFalse instances have the same hash
        return PhysicalFalse.class.hashCode();
    }

    @Nonnull
    @Override
    public String toString() {
        return "FALSE";
    }
}