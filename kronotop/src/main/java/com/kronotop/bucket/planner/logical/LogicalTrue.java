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

package com.kronotop.bucket.planner.logical;

import javax.annotation.Nonnull;

/**
 * Represents a logical constant TRUE.
 * Used in optimization passes when conditions are determined to always be true.
 */
public record LogicalTrue() implements LogicalNode {

    public static final LogicalTrue INSTANCE = new LogicalTrue();

    @Override
    public <R> R accept(LogicalPlanVisitor<R> visitor) {
        return visitor.visitTrue(this);
    }

    @Nonnull
    @Override
    public String toString() {
        return "TRUE";
    }
}