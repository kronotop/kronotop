// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket;

import com.kronotop.BaseClusterTest;
import com.kronotop.instance.KronotopInstance;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class BucketServiceTest extends BaseClusterTest {

    @Test
    void shardSelectorShouldReturnValidShard() {
        KronotopInstance instance = getInstances().getFirst();
        BucketService service = instance.getContext().getService(BucketService.NAME);

        BucketShard shard = assertDoesNotThrow(() -> service.getShardSelector().next());
        assertNotNull(shard);
    }
}
