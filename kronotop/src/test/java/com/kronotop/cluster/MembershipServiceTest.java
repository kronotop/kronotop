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

package com.kronotop.cluster;

import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MembershipServiceTest extends BaseClusterTest {

    @Test
    public void test_getLatestHeartbeats() {
        addNewInstance(); // Second
        addNewInstance(); // Third

        LinkedList<Member> members = new LinkedList<>();
        kronotopInstances.forEach((member, instance) -> {
            members.add(member);
        });

        MembershipService membershipService = kronotopInstances.get(members.getFirst()).getContext().getService(MembershipService.NAME);

        Map<Member, Long> latestHeartbeats = membershipService.getLatestHeartbeats(members.toArray(new Member[members.size()]));
        assertEquals(3, latestHeartbeats.size());

        kronotopInstances.forEach((member, instance) -> {
            assertTrue(latestHeartbeats.containsKey(member));
        });
    }
}
