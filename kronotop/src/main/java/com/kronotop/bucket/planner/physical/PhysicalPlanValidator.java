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

import java.util.List;

/**
 * Validates optimized physical plans by running a set of rules that detect query patterns
 * which would produce incorrect results at execution time.
 */
public final class PhysicalPlanValidator {

    private static final List<PhysicalPlanValidationRule> RULES = List.of(
            new UnsupportedSortRule()
    );

    private PhysicalPlanValidator() {
    }

    /**
     * Validates the physical plan against all registered rules.
     * Throws on the first violation found.
     *
     * @param context planner context with metadata and sort configuration
     * @param plan    the optimized physical plan to validate
     * @throws PhysicalPlanValidationException if any rule detects a violation
     */
    public static void validate(PlannerContext context, PhysicalNode plan) {
        for (PhysicalPlanValidationRule rule : RULES) {
            List<Violation> violations = rule.check(context, plan);
            if (!violations.isEmpty()) {
                throw new PhysicalPlanValidationException(violations.getFirst());
            }
        }
    }
}
