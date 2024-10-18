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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.kronotop.cluster.membership.MembershipService;
import com.kronotop.cluster.membership.NoSuchMemberException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;

public class Heartbeat {
    private static final byte[] HEARTBEAT_DELTA = new byte[]{1, 0, 0, 0, 0, 0, 0, 0}; // 1, byte order: little-endian

    public static long get(Transaction tr, Member member) {
        long heartbeat = 0;
        try {
            byte[] data = tr.get(member.getSubspace().pack(Member.HEARTBEAT_KEY)).join();
            if (data != null) {
                heartbeat = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong();
            }
        } catch (CompletionException e) {
            if (!(e.getCause() instanceof NoSuchDirectoryException)) {
                throw new NoSuchMemberException(String.format("No such member: %s", member.getExternalAddress()));
            }
        }
        return heartbeat;
    }

    public static void set(Transaction tr, Member member) {
        byte[] key = member.getSubspace().pack(Member.HEARTBEAT_KEY);
        tr.mutate(MutationType.ADD, key, HEARTBEAT_DELTA);
    }

    public static Map<Member, Long> get(Transaction tr, Member... members) {
        Map<Member, Long> result = new HashMap<>();
        for (Member member : members) {
            long latestHeartbeat = get(tr, member);
            result.put(member, latestHeartbeat);
        }
        return result;
    }
}
