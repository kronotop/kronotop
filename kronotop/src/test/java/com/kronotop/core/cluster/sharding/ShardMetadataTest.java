/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.core.cluster.sharding;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kronotop.core.cluster.coordinator.tasks.ReassignShardTask;
import com.kronotop.core.network.Address;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ShardMetadataTest {
    protected String expectedResult = "{\"tasks\":{},\"status\":\"OPERABLE\",\"owner\":{\"processId\":1,\"address\":{\"port\":5484,\"host\":\"localhost\"}}}";

    @Test
    public void testShardMetadata_encode() throws JsonProcessingException, UnknownHostException {
        Address address = Address.parseString("localhost:[5484]");
        ObjectMapper objectMapper = new ObjectMapper();
        ShardMetadata shardMetadata = new ShardMetadata(address, 1);
        shardMetadata.setStatus(ShardStatus.OPERABLE);
        String result = objectMapper.writeValueAsString(shardMetadata);
        assertEquals(expectedResult, result);
    }

    @Test
    public void testShardMetadata_decode() throws JsonProcessingException, UnknownHostException {
        Address address = Address.parseString("localhost:[5484]");
        ObjectMapper objectMapper = new ObjectMapper();
        ShardMetadata expectedShardMetadata = new ShardMetadata(address, 1);
        expectedShardMetadata.setStatus(ShardStatus.OPERABLE);

        ShardMetadata shardMetadata = objectMapper.readValue(expectedResult, ShardMetadata.class);
        assertEquals(expectedShardMetadata.getStatus(), shardMetadata.getStatus());
        assertEquals(expectedShardMetadata.getOwner().getAddress(), shardMetadata.getOwner().getAddress());
        assertEquals(expectedShardMetadata.getOwner().getProcessId(), shardMetadata.getOwner().getProcessId());
    }

    @Test
    public void testShardMetadata_tasks() throws UnknownHostException, JsonProcessingException {
        ShardOwner nextOwner = new ShardOwner(Address.parseString("localhost:[5585]"), 10);
        ReassignShardTask reassignShardTask = new ReassignShardTask(nextOwner, 3);

        Address address = Address.parseString("localhost:[5484]");
        ObjectMapper objectMapper = new ObjectMapper();
        ShardMetadata shardMetadata = new ShardMetadata(address, 1);
        shardMetadata.setStatus(ShardStatus.OPERABLE);
        ShardMetadata.Task task = new ShardMetadata.Task(reassignShardTask);
        shardMetadata.getTasks().put("foobar:10", task);
        String result = objectMapper.writeValueAsString(shardMetadata);

        String expectedResult = String.format("{\"tasks\":{\"foobar:10\":{\"task\":{\"shardId\":3,\"type\":\"REASSIGN_SHARD\"," +
                "\"createdAt\":%d,\"nextOwner\":{\"processId\":10,\"address\":{\"port\":5585,\"host\":\"localhost\"}}},\"completed\":false}}," +
                "\"status\":\"OPERABLE\",\"owner\":{\"processId\":1,\"address\":{\"port\":5484,\"host\":\"localhost\"}}}", reassignShardTask.getCreatedAt());
        assertEquals(expectedResult, result);
    }
}
