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

package com.kronotop.core.cluster.consistent;

import com.kronotop.common.utils.Utils;
import com.kronotop.core.cluster.Member;
import com.typesafe.config.Config;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.hash.Hashing.murmur3_32_fixed;

public class Consistent {
    private final int replicationFactor;
    private final double loadFactor;
    private final int numberOfShards;
    private final TreeSet<Integer> sortedSet = new TreeSet<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final HashMap<String, Double> loads = new HashMap<>();
    private final HashMap<String, Member> members = new HashMap<>();
    private final HashMap<Integer, Member> shards = new HashMap<>();
    private final HashMap<Integer, Member> ring = new HashMap<>();

    public Consistent(Config config, List<Member> members) {
        this(config);
        if (members != null && !members.isEmpty()) {
            for (Member member : members) {
                addMemberInternal(member);
            }
            distributeShards();
        }
    }

    public Consistent(Config config) {
        this.replicationFactor = config.getInt("cluster.consistent.replication_factor");
        this.loadFactor = config.getDouble("cluster.consistent.load_factor");
        this.numberOfShards = config.getInt("cluster.number_of_shards");
    }

    private int hash32(String key) {
        int hashedKey = murmur3_32_fixed().hashString(key, StandardCharsets.US_ASCII).asInt();
        return Utils.toPositive(hashedKey);
    }

    private double averageLoadInternal() {
        if (members.isEmpty()) {
            return 0;
        }
        double avgLoad = ((double) numberOfShards / members.size()) * loadFactor;
        return Math.ceil(avgLoad);
    }


    private void distributeWithLoad(int partID, Integer idx, HashMap<Integer, Member> newShards, HashMap<String, Double> newLoads) {
        double avgLoad = averageLoadInternal();
        int count = 0;
        while (true) {
            count++;
            if (count >= sortedSet.size()) {
                // User needs to decrease the number of shards, increase member count or increase load factor.
                throw new RuntimeException("not enough room to distribute shards");
            }

            Member member = ring.get(idx);
            if (member == null) {
                // TODO: ??
                throw new RuntimeException("member is missing");
            }
            Double load = newLoads.get(member.getId());
            if (load == null) {
                load = 0.0;
            }
            if (load + 1 <= avgLoad) {
                newShards.put(partID, member);
                newLoads.put(member.getId(), load + 1);
                return;
            }
            idx = sortedSet.ceiling(idx + 1);
            if (idx == null) {
                idx = sortedSet.first();
            }
        }
    }

    private void distributeShards() {
        HashMap<String, Double> newLoads = new HashMap<>();
        HashMap<Integer, Member> newShards = new HashMap<>();

        if (!members.isEmpty()) {
            for (int partID = 0; partID < numberOfShards; partID++) {
                int key = hash32(Integer.toString(partID));
                Integer idx = sortedSet.ceiling(key);
                if (idx == null) {
                    idx = sortedSet.first();
                }
                distributeWithLoad(partID, idx, newShards, newLoads);
            }
        }

        shards.clear();
        shards.putAll(newShards);

        loads.clear();
        loads.putAll(newLoads);
    }

    private void addMemberInternal(Member member) {
        for (int i = 0; i < replicationFactor; i++) {
            String key = String.format("%s%d", member.getId(), i);
            int h = hash32(key);
            ring.put(h, member);
            sortedSet.add(h);
        }
        // Storing member at this map is useful to find backup members of a shard.
        members.put(member.getId(), member);
    }

    // Add adds a new member to the consistent hash circle.
    public void addMember(Member member) {
        lock.writeLock().lock();
        try {
            if (members.containsKey(member.getId())) {
                // We already have this member. Quit immediately.
                return;
            }
            addMemberInternal(member);
            distributeShards();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int findShard(String key) {
        return hash32(key) % numberOfShards;
    }

    private Member getShardOwnerInternal(int shardId) {
        Member member = shards.get(shardId);
        if (member == null) {
            throw new NoShardOwnerFoundException();
        }
        return member;
    }

    public Member getShardOwner(int partID) {
        lock.readLock().lock();
        try {
            return getShardOwnerInternal(partID);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Member locate(String key) {
        int partID = findShard(key);
        return getShardOwner(partID);
    }

    public double averageLoad() {
        lock.readLock().lock();
        try {
            return averageLoadInternal();
        } finally {
            lock.readLock().unlock();
        }
    }

    public HashMap<Member, Double> loadDistribution() {
        lock.readLock().lock();
        try {
            HashMap<Member, Double> result = new HashMap<>();
            for (String id : members.keySet()) {
                Member member = members.get(id);
                Double load = loads.get(id);
                if (load == null) {
                    load = 0.0;
                }
                result.put(member, load);
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<Member> getMembers() {
        lock.readLock().lock();
        try {
            List<Member> result = new ArrayList<>();
            for (String id : members.keySet()) {
                result.add(members.get(id));
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void removeMember(Member member) {
        lock.writeLock().lock();
        try {
            if (!members.containsKey(member.getId())) {
                return;
            }
            for (int i = 0; i < replicationFactor; i++) {
                String key = String.format("%s%d", member.getId(), i);
                int h = hash32(key);
                ring.remove(h);
                sortedSet.remove(h);
            }
            members.remove(member.getId());
            distributeShards();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
