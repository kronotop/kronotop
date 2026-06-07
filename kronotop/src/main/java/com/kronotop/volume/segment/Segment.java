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

import java.io.IOException;

/**
 * Base interface for fixed-size, file-backed storage segments within a Volume.
 * Provides identity, configuration access, and lifecycle management.
 *
 * @see ReadableSegment
 * @see WritableSegment
 */
public interface Segment extends AutoCloseable {

    /**
     * Returns the configuration of this segment (id, data directory, and size).
     *
     * @return the segment configuration
     */
    SegmentConfig getConfig();

    /**
     * Returns the unique identifier of this segment.
     *
     * @return the segment id
     */
    long id();

    /**
     * Returns the total pre-allocated size of this segment in bytes.
     *
     * @return the segment size
     */
    long getSize();

    /**
     * Releases the underlying file handle and any associated resources.
     *
     * @throws IOException if an I/O error occurs while closing
     */
    @Override
    void close() throws IOException;

    /**
     * Closes the segment and deletes the underlying file from the disk.
     *
     * @return the absolute path of the deleted file
     * @throws IOException if an I/O error occurs
     */
    String destroy() throws IOException;
}
