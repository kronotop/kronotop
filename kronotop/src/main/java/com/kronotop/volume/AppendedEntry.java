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
 * AppendedEntry is a record class representing an entry that has been appended
 * in a segmented storage system. It encapsulates the version of the user data
 * and the metadata associated with the appended entry.
 * <p>
 * The `userVersion` field denotes a version identifier provided by the user,
 * which can be used to track changes or revisions of the stored entry.
 * <p>
 * The `metadata` field stores the {@link EntryMetadata} object, which contains
 * details such as the segment name, prefix, position, length, and other
 * attributes providing information about the entry's location and organization
 * within storage.
 */
public record AppendedEntry(int index, int userVersion, EntryMetadata metadata) {
}
