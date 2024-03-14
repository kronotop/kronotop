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


package com.kronotop.redis;

import com.kronotop.core.cluster.Member;

/**
 * The SlotRange class represents a range of hash slots in a distributed system.
 * It contains information about the shard ID, beginning slot number, ending slot number,
 * and the owner of the slot range.
 */

public class SlotRange {
    int shardId;
    int begin;
    int end;
    Member owner;

    public SlotRange(int begin) {
        this.begin = begin;
    }

    public void setOwner(Member owner) {
        this.owner = owner;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public void setShardId(int shardId) {
        this.shardId = shardId;
    }

    public Member getOwner() {
        return owner;
    }

    public int getEnd() {
        return end;
    }

    public int getBegin() {
        return begin;
    }

    public int getShardId() {
        return shardId;
    }

    @Override
    public String toString() {
        return String.format("SlotRange {begin=%d end=%d shardId=%d owner=%s}", begin, end, shardId, owner);
    }
}