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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

class MemberEvent extends BaseBroadcastEvent {
    private String memberId;
    private long createdAt;

    MemberEvent() {
    }

    public MemberEvent(BroadcastEventKind kind, String memberId, long createdAt) {
        super(kind);
        this.memberId = memberId;
        this.createdAt = createdAt;
    }

    public MemberEvent(BroadcastEventKind kind, String memberId) {
        super(kind);
        this.memberId = memberId;
        this.createdAt = Instant.now().toEpochMilli();
    }

    @JsonProperty
    public String memberId() {
        return memberId;
    }

    @JsonProperty
    public long createdAt() {
        return createdAt;
    }

    @Override
    public String toString() {
        return String.format(
                "MemberEvent {kind=%s id=%s createdAt=%d}",
                kind(),
                memberId,
                createdAt
        );
    }
}
