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

package com.kronotop.core.cluster;

import com.apple.foundationdb.tuple.Versionstamp;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.io.BaseEncoding;

import java.time.Instant;

public class MemberEvent {
    private EventTypes type;
    @JsonSerialize(using = ProcessIdSerializer.class)
    @JsonDeserialize(using = ProcessIdDeserializer.class)
    private Versionstamp processID;
    private long createdAt;
    private String host;
    private int port;

    MemberEvent() {
    }

    public MemberEvent(EventTypes type, String host, int port, Versionstamp processID, long createdAt) {
        this.type = type;
        this.processID = processID;
        this.createdAt = createdAt;
        this.host = host;
        this.port = port;
    }

    public MemberEvent(EventTypes type, String host, int port, Versionstamp processID) {
        this(type, host, port, processID, Instant.now().toEpochMilli());
    }

    public EventTypes getType() {
        return type;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public Versionstamp getProcessID() {
        return processID;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String toString() {
        return String.format(
                "MemberEvent {type=%s host=%s port=%d processID=%s, createdAt=%d}",
                type,
                host,
                port,
                BaseEncoding.base64().encode(processID.getBytes()),
                createdAt
        );
    }
}
