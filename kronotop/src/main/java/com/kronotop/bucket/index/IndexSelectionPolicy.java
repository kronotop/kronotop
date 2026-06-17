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

package com.kronotop.bucket.index;

/**
 * Filters indexes by {@link IndexStatus} during retrieval from
 * {@link SingleFieldIndexRegistry} and {@link CompoundIndexRegistry}.
 *
 * <ul>
 *   <li>{@link #READ} — only {@link IndexStatus#READY} indexes</li>
 *   <li>{@link #READWRITE} — {@link IndexStatus#BUILDING} or {@link IndexStatus#READY} indexes</li>
 *   <li>{@link #WRITABLE} — {@link IndexStatus#WAITING}, {@link IndexStatus#BUILDING}, or {@link IndexStatus#READY} indexes</li>
 *   <li>{@link #ALL} — all indexes regardless of status</li>
 * </ul>
 *
 * @see IndexStatus
 */
public enum IndexSelectionPolicy {
    /**
     * Selects only {@link IndexStatus#READY} indexes.
     *
     * <p>Use for read operations requiring fully built, stable indexes. Excludes indexes
     * still building or in error states.
     */
    READ,

    /**
     * Selects indexes in {@link IndexStatus#BUILDING} or {@link IndexStatus#READY} states.
     *
     * <p>Use for write operations that need to maintain indexes after boundary capture.
     * Excludes {@link IndexStatus#WAITING}, {@link IndexStatus#DROPPED}, and {@link IndexStatus#FAILED} indexes.
     */
    READWRITE,

    /**
     * Selects indexes in {@link IndexStatus#WAITING}, {@link IndexStatus#BUILDING}, or
     * {@link IndexStatus#READY} states.
     *
     * <p>Use for document write maintenance (insert, update, delete). An index becomes writable the
     * moment it durably exists, before the background boundary routine flips it to
     * {@link IndexStatus#BUILDING}. Combined with the boundary routine's pre-snapshot metadata
     * convergence, maintaining {@link IndexStatus#WAITING} indexes closes the window where a
     * concurrent write on a remote node would be indexed by neither the synchronous write path nor
     * the bounded background build.
     *
     * <p>Excludes {@link IndexStatus#DROPPED} and {@link IndexStatus#FAILED} indexes.
     */
    WRITABLE,

    /**
     * Selects all indexes regardless of status.
     *
     * <p>Use for administrative operations requiring visibility into all indexes,
     * including dropped or failed ones.
     */
    ALL
}
