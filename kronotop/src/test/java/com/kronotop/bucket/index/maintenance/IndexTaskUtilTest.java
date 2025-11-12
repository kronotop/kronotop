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

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for IndexTaskUtil covering task subspace management.
 */
class IndexTaskUtilTest extends BaseBucketHandlerTest {

    @Test
    void testOpenTasksSubspace() {
        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);

        assertNotNull(taskSubspace, "Task subspace should not be null");
        assertNotNull(taskSubspace.getKey(), "Task subspace key should not be null");
        assertTrue(taskSubspace.getKey().length > 0, "Task subspace key should not be empty");
    }

    @Test
    void testOpenTasksSubspaceForMultipleShards() {
        DirectorySubspace subspace0 = IndexTaskUtil.openTasksSubspace(context, 0);
        DirectorySubspace subspace1 = IndexTaskUtil.openTasksSubspace(context, 1);
        DirectorySubspace subspace2 = IndexTaskUtil.openTasksSubspace(context, 2);

        assertNotNull(subspace0);
        assertNotNull(subspace1);
        assertNotNull(subspace2);

        assertNotEquals(subspace0.getKey(), subspace1.getKey(),
                "Different shards should have different subspaces");
        assertNotEquals(subspace1.getKey(), subspace2.getKey(),
                "Different shards should have different subspaces");
        assertNotEquals(subspace0.getKey(), subspace2.getKey(),
                "Different shards should have different subspaces");
    }

    @Test
    void testOpenTasksSubspaceConsistency() {
        DirectorySubspace subspace1 = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        DirectorySubspace subspace2 = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);

        assertNotNull(subspace1);
        assertNotNull(subspace2);
        assertArrayEquals(subspace1.getKey(), subspace2.getKey(),
                "Opening the same shard subspace should return consistent results");
    }
}
