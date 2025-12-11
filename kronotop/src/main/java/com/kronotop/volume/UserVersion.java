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

package com.kronotop.volume;

/**
 * UserVersion manages the user-defined version component of FoundationDB versionstamped keys within a VolumeSession.
 *
 * <p><b>FoundationDB Versionstamp Structure:</b></p>
 * <p>FoundationDB versionstamps are 12-byte unique, monotonically increasing values consisting of:</p>
 * <ul>
 *   <li><b>Transaction version (10 bytes)</b>: Assigned by FoundationDB when the transaction commits</li>
 *   <li><b>User version (2 bytes)</b>: Application-defined value to order entries within a single transaction</li>
 * </ul>
 *
 * <p><b>Role in Volume:</b></p>
 * <p>UserVersion provides the 2-byte user version component that enables ordering multiple entries
 * appended within a single transaction. Each VolumeSession maintains its own UserVersion instance
 * to track how many entries have been written in the current transaction.</p>
 *
 * <p><b>Key Characteristics:</b></p>
 * <ul>
 *   <li><b>Range</b>: 0x0000 to 0xFFFF (0 to 65535 decimal)</li>
 *   <li><b>Session-scoped</b>: Each VolumeSession has its own independent counter</li>
 *   <li><b>Sequential ordering</b>: Starts at 0 and increments with each entry</li>
 *   <li><b>Limit enforcement</b>: Throws {@link TooManyEntriesException} if exceeded</li>
 * </ul>
 *
 * <p><b>Usage Flow:</b></p>
 * <pre>{@code
 * // Within a transaction, multiple entries are appended
 * VolumeSession session = new VolumeSession(transaction, prefix);
 * volume.append(session, entry1, entry2, entry3);
 *
 * // Internally, each entry gets a sequential user version:
 * // entry1 -> user version 0
 * // entry2 -> user version 1
 * // entry3 -> user version 2
 *
 * // After commit, FoundationDB assigns the transaction version (e.g., 0x123456789A)
 * // Final versionstamped keys:
 * // entry1 -> 0x123456789A0000
 * // entry2 -> 0x123456789A0001
 * // entry3 -> 0x123456789A0002
 * }</pre>
 *
 * <p><b>Thread Safety:</b></p>
 * <p>The {@link #getAndIncrement()} method is synchronized to ensure thread-safe access
 * when multiple threads might access the same VolumeSession concurrently.</p>
 *
 * <p><b>Limitations:</b></p>
 * <p>The maximum value of 65,535 (0xFFFF) limits the number of entries that can be
 * appended in a single transaction. Attempting to exceed this limit throws
 * {@link TooManyEntriesException}.</p>
 *
 * @see VolumeSession
 * @see Volume#append(VolumeSession, java.nio.ByteBuffer...)
 * @see TooManyEntriesException
 */
final class UserVersion {
    /**
     * Minimum user version value (0x0000).
     */
    static final int MIN_VALUE = 0x0;

    /**
     * Maximum user version value (0xFFFF = 65535).
     * Represents the maximum number of entries that can be appended in a single transaction.
     */
    static final int MAX_VALUE = 0xffff;

    /**
     * The current user version counter, starts at MIN_VALUE and increments with each entry.
     */
    private int userVersion = MIN_VALUE;

    /**
     * Returns the current user version and increments it atomically.
     *
     * <p>This method is called once for each entry written within a VolumeSession
     * to assign sequential user versions for proper ordering within a transaction.</p>
     *
     * <p><b>Thread Safety:</b></p>
     * <p>Synchronized to ensure atomic read-and-increment operations when accessed
     * concurrently.</p>
     *
     * @return the current user version before incrementing
     * @throws TooManyEntriesException if the user version exceeds MAX_VALUE (65535),
     *                                 indicating too many entries in a single transaction
     */
    synchronized int getAndIncrement() {
        try {
            if (userVersion > MAX_VALUE) {
                throw new TooManyEntriesException();
            }
            return userVersion;
        } finally {
            userVersion++;
        }
    }
}
