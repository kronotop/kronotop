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

import com.kronotop.BaseClusterTest;
import com.kronotop.KronotopTestInstance;
import org.junit.jupiter.api.Test;

import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MembershipServiceIntegrationTest extends BaseClusterTest {

    @Test
    public void test_getLatestHeartbeat() {
        KronotopTestInstance instance = getInstances().getFirst();
        MembershipService membership = instance.getContext().getService(MembershipService.NAME);
        await().atMost(5, TimeUnit.SECONDS).until(() -> membership.getLatestHeartbeat(instance.getContext().getMember()) > 0);
    }

    @Test
    public void test_listMembers() {
        addNewInstance();

        KronotopTestInstance instance = getInstances().getFirst();
        MembershipService membership = instance.getContext().getService(MembershipService.NAME);
        TreeSet<Member> members = membership.listMembers();
        assertEquals(2, members.size());
    }

    @Test
    public void test_member_shutdown_then_check_MemberStatus() {
        addNewInstance();

        KronotopTestInstance first = getInstances().getFirst();

        first.shutdownWithoutCleanup();
        kronotopInstances.remove(first.getMember());

        KronotopTestInstance second = getInstances().getFirst();
        MembershipService membership = second.getContext().getService(MembershipService.NAME);
        TreeSet<Member> members = membership.listMembers();
        assertEquals(2, members.size());

        MemberStatus status = MemberStatus.UNKNOWN;
        for (Member member : members) {
            if (member.equals(first.getMember())) {
                status = member.getStatus();
            }
        }
        assertEquals(MemberStatus.STOPPED, status);
    }
}
