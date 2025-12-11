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

package com.kronotop.cluster.client.protocol;

import java.util.Optional;

/**
 * Represents a changelog entry from the CHANGELOGRANGE command response.
 */
public record ChangeLogEntryResponse(
        String versionstamp,
        String kind,
        long prefix,
        ChangeLogCoordinateResponse before,
        ChangeLogCoordinateResponse after
) {
    /**
     * @return true if this entry has a before coordinate (UPDATE or DELETE operations)
     */
    public boolean hasBefore() {
        return before != null;
    }

    /**
     * @return true if this entry has an after coordinate (APPEND or UPDATE operations)
     */
    public boolean hasAfter() {
        return after != null;
    }

    /**
     * @return the before coordinate wrapped in Optional
     */
    public Optional<ChangeLogCoordinateResponse> getBefore() {
        return Optional.ofNullable(before);
    }

    /**
     * @return the after coordinate wrapped in Optional
     */
    public Optional<ChangeLogCoordinateResponse> getAfter() {
        return Optional.ofNullable(after);
    }
}
