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
 * Validation rule that checks a physical query plan for patterns that would produce incorrect results.
 */
public interface PhysicalPlanValidationRule {

    /**
     * Checks the physical plan for violations.
     *
     * @param context planner context with metadata and sort configuration
     * @param plan    the optimized physical plan to validate
     * @return list of violations found (empty if the plan is valid)
     */
    List<Violation> check(PlannerContext context, PhysicalNode plan);

    /**
     * Returns the name of this validation rule.
     */
    String getName();
}
