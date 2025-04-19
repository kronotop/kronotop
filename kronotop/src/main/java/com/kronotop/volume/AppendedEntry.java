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
 * AppendedEntry is an immutable representation of an entry appended to a segmented storage system.
 * It is primarily used to encapsulate information about the appended entry, including its index,
 * the user-defined version of the entry, and metadata describing its properties.
 * <p>
 * The class consists of:
 * - An `index` indicating the order or position of the appended entry.
 * - A `userVersion` representing the user-defined version of the entry, typically used for
 * tracking or versioning purposes.
 * - An `EntryMetadata` object encapsulating detailed information such as segment name, prefix,
 * position, and length of the entry in the storage.
 * <p>
 * This design ensures immutability and is suitable for applications requiring thread safety
 * and reliable encapsulation of entry data.
 */
public record AppendedEntry(int index, int userVersion, EntryMetadata metadata, byte[] encodedMetadata) {
}
