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


package com.kronotop.redis;

import com.kronotop.cluster.Member;

import java.util.Set;

/**
 * The SlotRange class represents a range of hash slots in a distributed system.
 * It contains information about the shard ID, beginning slot number, ending slot number,
 * and the owner of the slot range.
 */

public class SlotRange {
    private final int begin;
    private int shardId;
    private int end;
    private Member primary;
    private Set<Member> standbys;

    public SlotRange(int begin) {
        this.begin = begin;
    }

    public Member getPrimary() {
        return primary;
    }

    public void setPrimary(Member owner) {
        this.primary = owner;
    }

    public Set<Member> getStandbys() {
        return standbys;
    }

    public void setStandbys(Set<Member> standbys) {
        this.standbys = standbys;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public int getBegin() {
        return begin;
    }

    public int getShardId() {
        return shardId;
    }

    public void setShardId(int shardId) {
        this.shardId = shardId;
    }

    @Override
    public String toString() {
        return String.format("SlotRange {begin=%d end=%d shardId=%d primary=%s}", begin, end, shardId, primary);
    }
}