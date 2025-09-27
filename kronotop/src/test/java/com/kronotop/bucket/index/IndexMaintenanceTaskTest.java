/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.bucket.index;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.internal.JSONUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IndexMaintenanceTaskTest {
    @Test
    void shouldEncodeDecode() {
        IndexMaintenanceTask task = new IndexMaintenanceTask(
                "namespace-name",
                "bucket-name",
                12345
        );
        byte[] encoded = JSONUtil.writeValueAsBytes(task);
        IndexMaintenanceTask decoded = JSONUtil.readValue(encoded, IndexMaintenanceTask.class);
        assertEquals(task.getNamespace(), decoded.getNamespace());
        assertEquals(task.getBucket(), decoded.getBucket());
        assertEquals(task.getIndexId(), decoded.getIndexId());
        assertFalse(task.isCompleted());
        assertFalse(decoded.isCompleted());
        assertNull(task.getHighestVersionstamp());
        assertNull(decoded.getHighestVersionstamp());
    }

    @Test
    void shouldEncodeDecode_completed() {
        IndexMaintenanceTask task = new IndexMaintenanceTask(
                "namespace-name",
                "bucket-name",
                12345
        );
        task.setCompleted(true);
        byte[] encoded = JSONUtil.writeValueAsBytes(task);
        IndexMaintenanceTask decoded = JSONUtil.readValue(encoded, IndexMaintenanceTask.class);
        assertTrue(task.isCompleted());
        assertTrue(decoded.isCompleted());
    }

    @Test
    void shouldEncodeDecode_end() {
        IndexMaintenanceTask task = new IndexMaintenanceTask(
                "namespace-name",
                "bucket-name",
                12345
        );
        Versionstamp end = Versionstamp.incomplete(1);
        task.setHighestVersionstamp(end);
        byte[] encoded = JSONUtil.writeValueAsBytes(task);
        IndexMaintenanceTask decoded = JSONUtil.readValue(encoded, IndexMaintenanceTask.class);
        assertEquals(end, task.getHighestVersionstamp());
        assertEquals(end, decoded.getHighestVersionstamp());
    }
}