/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop.cluster.sharding;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kronotop.cluster.MockProcessIdGeneratorImpl;
import com.kronotop.cluster.coordinator.tasks.ReassignShardTask;
import com.kronotop.network.Address;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;

public class ShardMetadataTest {
    @Test
    public void test_encode_decode() throws UnknownHostException, JsonProcessingException {
        MockProcessIdGeneratorImpl processIdGenerator = new MockProcessIdGeneratorImpl();
        ShardOwner nextOwner = new ShardOwner(Address.parseString("localhost:[5585]"), processIdGenerator.getProcessID());
        ReassignShardTask reassignShardTask = new ReassignShardTask(nextOwner, 3);

        Address address = Address.parseString("localhost:[5484]");
        ObjectMapper objectMapper = new ObjectMapper();
        ShardMetadata shardMetadata = new ShardMetadata(address, processIdGenerator.getProcessID());
        ShardMetadata.Task task = new ShardMetadata.Task(reassignShardTask);
        shardMetadata.getTasks().put(task.getBaseTask().getTaskId(), task);

        String encodedShardMetadata = objectMapper.writeValueAsString(shardMetadata);
        ShardMetadata decodedShardMetadata = objectMapper.readValue(encodedShardMetadata, ShardMetadata.class);
        assertThat(shardMetadata).usingRecursiveComparison().isEqualTo(decodedShardMetadata);
    }
}
