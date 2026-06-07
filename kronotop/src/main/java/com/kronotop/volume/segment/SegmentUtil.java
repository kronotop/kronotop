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

package com.kronotop.volume.segment;

import com.google.common.base.Strings;

import java.nio.file.Path;

public class SegmentUtil {
    public static String DIRECTORY = "segments";

    /**
     * Generates a zero-padded file name for the given segment id.
     *
     * @param id the segment id
     * @return a 19-character zero-padded string representation
     */
    public static String generateFileName(long id) {
        return Strings.padStart(Long.toString(id), 19, '0');
    }

    /**
     * Resolves the file path for a segment within the given data directory.
     *
     * @param dataDir the root data directory
     * @param id      the segment id
     * @return the full path to the segment file
     */
    public static Path getFilePath(String dataDir, long id) {
        return Path.of(dataDir, DIRECTORY, generateFileName(id));
    }
}
