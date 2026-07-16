/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.instance;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KronotopInstanceStarterTest {

    // Behavior: formatBytes renders each unit boundary as a short human-readable string.
    @Test
    void shouldFormatBytesAcrossUnits() {
        assertEquals("512 B", KronotopInstanceStarter.formatBytes(512));
        assertEquals("1023 B", KronotopInstanceStarter.formatBytes(1023));
        assertEquals("1 KB", KronotopInstanceStarter.formatBytes(1024));
        assertEquals("1 MB", KronotopInstanceStarter.formatBytes(1024L * 1024));
        assertEquals("512 MB", KronotopInstanceStarter.formatBytes(512L * 1024 * 1024));
        assertEquals("4.0 GB", KronotopInstanceStarter.formatBytes(4L * 1024 * 1024 * 1024));
        assertEquals("1.5 GB", KronotopInstanceStarter.formatBytes(3L * 512 * 1024 * 1024));
    }

    // Behavior: resolveProperty falls back to "unknown" for missing or unresolved build placeholders.
    @Test
    void shouldResolveMissingOrUnresolvedProperty() {
        assertEquals("unknown", KronotopInstanceStarter.resolveProperty(null));
        assertEquals("unknown", KronotopInstanceStarter.resolveProperty(""));
        assertEquals("unknown", KronotopInstanceStarter.resolveProperty("   "));
        assertEquals("unknown", KronotopInstanceStarter.resolveProperty("${git.commit.id.abbrev}"));
        assertEquals("a1b2c3d", KronotopInstanceStarter.resolveProperty("a1b2c3d"));
    }
}
