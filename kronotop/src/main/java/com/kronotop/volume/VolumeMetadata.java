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

import java.util.List;

/**
 * Immutable snapshot of a volume's metadata: its unique identifier, current status, and registered segment IDs.
 *
 * @param id         unique volume identifier derived from a SipHash24 of a random UUID
 * @param status     current read/write status of the volume
 * @param segmentIds sorted list of segment IDs belonging to this volume
 */
public record VolumeMetadata(long id, VolumeStatus status, List<Long> segmentIds) {
}
