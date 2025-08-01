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

import com.kronotop.volume.segment.Segment;

public class VolumeTestUtil {

    public static EntryMetadata generateEntryMetadata(int segmentId, long position, long length, String prefixName) {
        Prefix prefix = new Prefix(prefixName);
        // Initialize necessary data
        String segment = Segment.generateName(segmentId);
        int id = EntryMetadataIdGenerator.generate(segmentId, position);
        // Create EntryMetadata instance
        return new EntryMetadata(segment, prefix.asBytes(), position, length, id);
    }
}
