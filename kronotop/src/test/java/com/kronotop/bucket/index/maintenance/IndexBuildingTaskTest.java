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

package com.kronotop.bucket.index.maintenance;

import com.kronotop.TestUtil;
import com.kronotop.internal.JSONUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IndexBuildingTaskTest {
    @Test
    void shouldEncodeDecode() {
        IndexBuildingTask task = new IndexBuildingTask(
                "namespace-name",
                "bucket-name",
                12345,
                1,
                TestUtil.generateVersionstamp(1),
                TestUtil.generateVersionstamp(2)
        );
        byte[] encoded = JSONUtil.writeValueAsBytes(task);
        IndexBuildingTask decoded = JSONUtil.readValue(encoded, IndexBuildingTask.class);
        assertEquals(IndexMaintenanceTaskKind.BUILD, task.getKind());
        assertEquals(task.getNamespace(), decoded.getNamespace());
        assertEquals(task.getBucket(), decoded.getBucket());
        assertEquals(task.getIndexId(), decoded.getIndexId());
        assertEquals(task.getShardId(), decoded.getShardId());
        assertEquals(task.getKind(), decoded.getKind());
        assertEquals(task.getLower(), decoded.getLower());
        assertEquals(task.getUpper(), decoded.getUpper());
    }
}