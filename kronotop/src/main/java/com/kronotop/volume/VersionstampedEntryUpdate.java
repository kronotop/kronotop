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

package com.kronotop.volume;

import com.apple.foundationdb.tuple.Versionstamp;

import java.nio.ByteBuffer;

/**
 * Bundles a versionstamp key, its current entry metadata, and new entry data for an update
 * that requires no FoundationDB lookups.
 *
 * @param key      the versionstamp identifying the entry to update
 * @param metadata the current entry metadata (before the update)
 * @param entry    the new data to replace the existing entry with
 */
public record VersionstampedEntryUpdate(Versionstamp key, EntryMetadata metadata, ByteBuffer entry) {
}
