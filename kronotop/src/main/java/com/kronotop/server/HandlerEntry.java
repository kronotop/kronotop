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

package com.kronotop.server;

/**
 * Immutable container for a command handler and its metadata.
 * Pre-cached parameter constraints eliminate reflection overhead during request processing.
 *
 * @param handler               the handler instance
 * @param commandType           the command type enum constant
 * @param minimumParameterCount minimum required parameters (-1 = no constraint)
 * @param maximumParameterCount maximum allowed parameters (-1 = no constraint)
 */
public record HandlerEntry(
        Handler handler,
        CommandType commandType,
        int minimumParameterCount,
        int maximumParameterCount
) {
    /**
     * Constant indicating no parameter constraint is set.
     */
    public static final int NO_CONSTRAINT = -1;

    public boolean hasMinimumParameterCount() {
        return minimumParameterCount != NO_CONSTRAINT;
    }

    public boolean hasMaximumParameterCount() {
        return maximumParameterCount != NO_CONSTRAINT;
    }
}
