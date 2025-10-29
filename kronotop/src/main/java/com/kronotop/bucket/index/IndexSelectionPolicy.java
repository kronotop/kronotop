/*
 * Copyright (c) 2023-2025 Burak Sezer
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
 * Controls index selection filtering based on index status during retrieval operations.
 *
 * <p>This enum determines which indexes are accessible based on their current {@link IndexStatus}.
 * It's used by {@link IndexRegistry} to filter indexes according to caller requirements.
 *
 * <p><b>Policy Rules:</b>
 * <ul>
 *   <li>{@link #READ}: Only {@link IndexStatus#READY} indexes (fully built and queryable)</li>
 *   <li>{@link #READWRITE}: {@link IndexStatus#WAITING}, {@link IndexStatus#BUILDING},
 *       or {@link IndexStatus#READY} indexes (actively maintained)</li>
 *   <li>{@link #ALL}: Any index regardless of status, including {@link IndexStatus#DROPPED}
 *       and {@link IndexStatus#FAILED}</li>
 * </ul>
 *
 * <p><b>Usage Context:</b>
 * <ul>
 *   <li><b>Query execution:</b> Use {@link #READ} to ensure stable, complete index data</li>
 *   <li><b>Write operations:</b> Use {@link #READWRITE} to maintain indexes during document changes</li>
 *   <li><b>Maintenance/admin:</b> Use {@link #ALL} to access any index for status checks or cleanup</li>
 * </ul>
 *
 * @see IndexRegistry#getIndex(String, IndexSelectionPolicy)
 * @see IndexRegistry#getIndexById(long, IndexSelectionPolicy)
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
     * Selects indexes in {@link IndexStatus#WAITING}, {@link IndexStatus#BUILDING},
     * or {@link IndexStatus#READY} states.
     *
     * <p>Use for write operations that need to maintain indexes regardless of build status.
     * Excludes {@link IndexStatus#DROPPED} and {@link IndexStatus#FAILED} indexes.
     */
    READWRITE,

    /**
     * Selects all indexes regardless of status.
     *
     * <p>Use for administrative operations requiring visibility into all indexes,
     * including dropped or failed ones.
     */
    ALL
}
