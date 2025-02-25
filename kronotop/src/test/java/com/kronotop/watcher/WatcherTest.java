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

package com.kronotop.watcher;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class WatcherTest {

    @Test
    public void testWatcher_watchKey() {
        Watcher w = new Watcher();
        Long version = w.watchKey(1L, "mykey");
        assertEquals(0, version);
        assertTrue(w.hasWatchers());
    }

    @Test
    public void testWatcher_watchKey_Subsequent_Calls() {
        Watcher w = new Watcher();

        long versionOne = w.watchKey(1L, "mykey");
        assertEquals(0, versionOne);

        Long versionTwo = w.watchKey(1L, "mykey");
        assertEquals(0, versionTwo);

        assertEquals(versionTwo, versionOne);
        assertTrue(w.hasWatchers());
    }

    @Test
    public void testWatcher_unwatchKey() {
        Watcher w = new Watcher();
        w.watchKey(1L, "mykey");
        w.unwatchKey(1L, "mykey");
        assertFalse(w.hasWatchers());
    }

    @Test
    public void testWatcher_idempotency() {
        Watcher w = new Watcher();
        w.watchKey(1L, "mykey");
        w.watchKey(1L, "mykey");
        w.watchKey(1L, "mykey");

        w.unwatchKey(1L, "mykey");

        assertFalse(w.hasWatchers());
    }

    @Test
    public void testWatcher_Many_Watchers() {
        Watcher w = new Watcher();
        w.watchKey(1L, "mykey");

        w.watchKey(2L, "mykey");
        w.unwatchKey(1L, "mykey");
        assertTrue(w.hasWatchers());
    }

    @Test
    public void testWatcher_increaseWatchedKeyVersion() {
        Watcher w = new Watcher();

        Long version = w.watchKey(1L, "mykey");
        w.increaseWatchedKeyVersion("mykey");
        Boolean result = w.isModified("mykey", version);
        assertTrue(result);
    }
}
