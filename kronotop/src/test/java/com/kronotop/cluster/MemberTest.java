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

import com.apple.foundationdb.tuple.Versionstamp;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kronotop.network.Address;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MemberTest {
    private final MockProcessIdGeneratorImpl processIdGenerator = new MockProcessIdGeneratorImpl();

    @Test
    public void test_Member() throws UnknownHostException, JsonProcessingException {
        Member member = createMember("localhost:[5484]");
        ObjectMapper objectMapper = new ObjectMapper();
        String serialized = objectMapper.writeValueAsString(member);
        Member deserialized = objectMapper.readValue(serialized, Member.class);
        assertEquals(member, deserialized);
    }

    @Test
    public void test_Members_compareTo() throws UnknownHostException, JsonProcessingException {
        Member memberOne = createMember("localhost:[5484]");
        Member memberTwo = createMember("localhost:[5585]");
        assertTrue(memberOne.getProcessId().compareTo(memberTwo.getProcessId()) < 0);
    }

    @Test
    public void test_Members_sorting() throws UnknownHostException, JsonProcessingException {
        Member memberOne = createMember("localhost:[5484]");
        Member memberTwo = createMember("localhost:[5585]");
        Member memberThree = createMember("localhost:[5686]");

        TreeSet<Member> members = new TreeSet<>(Comparator.comparing(Member::getProcessId));
        members.add(memberThree);
        members.add(memberOne);
        members.add(memberTwo);

        assertEquals(memberOne, members.first());
        assertEquals(memberTwo, members.higher(memberOne));
        assertEquals(memberThree, members.last());
    }

    private Member createMember(String addressString) throws UnknownHostException {
        Versionstamp processId = processIdGenerator.getProcessID();
        Address address = Address.parseString(addressString);
        return new Member(MemberIdGenerator.generateId(), address, address, processId);
    }
}
