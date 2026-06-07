/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.volume.changelog;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.volume.OperationKind;

import java.util.Optional;

/**
 * Represents a single entry in the volume changelog, tracking an APPEND, INSERT, UPDATE, or DELETE operation.
 *
 * <p>Each entry carries before/after segment coordinates depending on the operation kind:
 * <ul>
 *   <li>APPEND/INSERT: only {@code after} is present (new data location)</li>
 *   <li>UPDATE: both {@code before} (old location) and {@code after} (new location) are present</li>
 *   <li>DELETE: only {@code before} is present (removed data location)</li>
 * </ul>
 */
public class ChangeLogEntry {

    private final OperationKind kind;
    private final Versionstamp versionstamp;
    private final ChangeLogCoordinate before;
    private final ChangeLogCoordinate after;
    private final long prefix;

    ChangeLogEntry(Versionstamp versionstamp, OperationKind kind, ChangeLogCoordinate before, ChangeLogCoordinate after, long prefix) {
        this.versionstamp = versionstamp;
        this.kind = kind;
        this.before = before;
        this.after = after;
        this.prefix = prefix;
    }

    /**
     * Returns the segment coordinate before the operation, if applicable.
     *
     * @return coordinate for DELETE and UPDATE operations, empty for APPEND and INSERT
     */
    public Optional<ChangeLogCoordinate> getBefore() {
        return Optional.ofNullable(before);
    }

    /**
     * Returns the segment coordinate after the operation, if applicable.
     *
     * @return coordinate for APPEND, INSERT, and UPDATE operations, empty for DELETE
     */
    public Optional<ChangeLogCoordinate> getAfter() {
        return Optional.ofNullable(after);
    }

    /**
     * Returns the prefix associated with this changelog entry.
     */
    public long getPrefix() {
        return prefix;
    }

    /**
     * Returns the versionstamp identifying the entry this operation applies to.
     */
    public Versionstamp getVersionstamp() {
        return versionstamp;
    }

    public boolean hasBefore() {
        return before != null;
    }

    public boolean hasAfter() {
        return after != null;
    }

    /**
     * Returns the operation kind (APPEND, INSERT, UPDATE, or DELETE).
     */
    public OperationKind getKind() {
        return kind;
    }
}
