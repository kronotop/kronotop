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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents the context information used during the vacuum process for a specific segment.
 * This class encapsulates details such as the segment name, the read version of the data,
 * and a flag for the termination signal.
 */
public class VacuumContext {
    private final String segment;
    private final long readVersion;
    private final AtomicBoolean stop;

    VacuumContext(String segment, long readVersion, AtomicBoolean stop) {
        this.segment = segment;
        this.readVersion = readVersion;
        this.stop = stop;
    }

    String segment() {
        return segment;
    }

    long readVersion() {
        return readVersion;
    }

    boolean stop() {
        return stop.get();
    }
}
