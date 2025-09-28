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

import com.apple.foundationdb.tuple.Versionstamp;

import java.nio.ByteBuffer;

/**
 * Represents a fully materialized entry in a Volume.
 * <p>
 * A {@code VolumeEntry} bundles together:
 * <ul>
 *   <li>{@code key}: the logical address of the entry, expressed as a {@link Versionstamp},</li>
 *   <li>{@code entry}: the raw payload as stored in the volume,</li>
 *   <li>{@code metadata}: encoded {@code EntryMetadata} describing the physical layout
 *       (e.g., offset, length, checksum, or other volume-specific information).</li>
 * </ul>
 *
 * <p>This type is returned by {@code VolumeIterator}, which scans over the append-only
 * log segments of a shard and decodes each entry into its logical key, data, and
 * associated metadata. Consumers such as index builders, garbage collection, or
 * repair tasks can rely on {@code metadata} to make decisions that require knowledge
 * of the entry’s physical representation.</p>
 *
 * <p>For simple read/write operations where metadata is not needed, higher-level APIs
 * may expose lighter abstractions (e.g., {@code KeyEntry}).</p>
 */
public record VolumeEntry(Versionstamp key, ByteBuffer entry, byte[] metadata) {
}
