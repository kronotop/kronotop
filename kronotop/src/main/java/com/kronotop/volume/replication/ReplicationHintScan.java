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

package com.kronotop.volume.replication;

import java.util.List;

/**
 * Result of one hint scan over a replicated byte range.
 *
 * @param hints  hints of the entries that are fully inside the scanned range
 * @param cursor the start position of the next scan: the first incomplete entry, or the range end
 */
public record ReplicationHintScan(List<ReplicationHintEntry> hints, long cursor) {
}
