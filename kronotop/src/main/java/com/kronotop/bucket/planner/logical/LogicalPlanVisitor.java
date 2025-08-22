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

/**
 * LogicalPlanVisitor defines a visitor pattern interface for traversing and processing logical plan nodes.
 * Each method corresponds to a specific type of logical node in the hierarchical structure of the logical plan.
 * Implementers of this interface can define how each node type should be processed.
 *
 * @param <R> The type of the result produced by each visit method.
 */
public interface LogicalPlanVisitor<R> {
    R visitFilter(LogicalFilter filter);

    R visitAnd(LogicalAnd and);

    R visitOr(LogicalOr or);

    R visitNot(LogicalNot not);

    R visitElemMatch(LogicalElemMatch elemMatch);

    R visitTrue(LogicalTrue logicalTrue);

    R visitFalse(LogicalFalse logicalFalse);
}
