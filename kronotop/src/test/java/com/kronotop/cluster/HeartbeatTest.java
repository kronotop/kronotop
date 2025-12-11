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

package com.kronotop.cluster;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseStandaloneInstanceTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HeartbeatTest extends BaseStandaloneInstanceTest {
    private DirectorySubspace testSubspace;

    @BeforeEach
    void setupTestSubspace() {
        testSubspace = createOrOpenSubspaceUnderCluster("heartbeat-test");
    }

    @Test
    void shouldReturnZeroWhenHeartbeatNotSet() {
        long heartbeat = context.getFoundationDB().run(tr -> Heartbeat.get(tr, testSubspace));
        assertEquals(0, heartbeat);
    }

    @Test
    void shouldIncrementHeartbeat() {
        context.getFoundationDB().run(tr -> {
            Heartbeat.set(tr, testSubspace);
            return null;
        });

        long heartbeat = context.getFoundationDB().run(tr -> Heartbeat.get(tr, testSubspace));
        assertEquals(1, heartbeat);
    }

    @Test
    void shouldIncrementHeartbeatMultipleTimes() {
        for (int i = 0; i < 3; i++) {
            context.getFoundationDB().run(tr -> {
                Heartbeat.set(tr, testSubspace);
                return null;
            });
        }

        long heartbeat = context.getFoundationDB().run(tr -> Heartbeat.get(tr, testSubspace));
        assertEquals(3, heartbeat);
    }

    @Test
    void shouldGetCorrectHeartbeatAcrossTransactions() {
        context.getFoundationDB().run(tr -> {
            Heartbeat.set(tr, testSubspace);
            return null;
        });

        long heartbeat1 = context.getFoundationDB().run(tr -> Heartbeat.get(tr, testSubspace));
        assertEquals(1, heartbeat1);

        context.getFoundationDB().run(tr -> {
            Heartbeat.set(tr, testSubspace);
            return null;
        });

        long heartbeat2 = context.getFoundationDB().run(tr -> Heartbeat.get(tr, testSubspace));
        assertEquals(2, heartbeat2);
    }
}
