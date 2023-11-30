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
import com.kronotop.core.cluster.coordinator.tasks.AssignShardTask;
import com.kronotop.core.network.Address;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AssignShardTaskTest {
    @Test
    public void testAssignShardTask() throws JsonProcessingException, UnknownHostException {
        ShardOwner nextOwner = new ShardOwner(Address.parseString("localhost:[5585]"), 10);
        AssignShardTask assignShardTask = new AssignShardTask(3);

        ObjectMapper objectMapper = new ObjectMapper();
        String encoded = objectMapper.writeValueAsString(assignShardTask);
        AssignShardTask decoded = objectMapper.readValue(encoded, AssignShardTask.class);
        assertEquals(decoded.toString().trim(), assignShardTask.toString().trim());
    }
}