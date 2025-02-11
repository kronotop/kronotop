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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.kronotop.VersionstampUtils;
import com.kronotop.network.Address;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;

import static com.google.common.hash.Hashing.sipHash24;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Member {
    @JsonIgnore
    public static final String HEARTBEAT_KEY = "heartbeat";

    private String id;
    private MemberStatus status = MemberStatus.UNKNOWN;
    private Address externalAddress;
    private Address internalAddress;
    private int hashCode;
    @JsonSerialize(using = ProcessIdSerializer.class)
    @JsonDeserialize(using = ProcessIdDeserializer.class)
    private Versionstamp processId;

    Member() {
    }

    public Member(
            @Nonnull String id,
            @Nonnull Address externalAddress,
            @Nonnull Address internalAddress,
            @Nonnull Versionstamp processId
    ) {
        if (id.isBlank()) {
            throw new IllegalArgumentException("id cannot be blank");
        }
        this.id = id;
        this.externalAddress = externalAddress;
        this.internalAddress = internalAddress;
        this.processId = processId;
        this.hashCode = sipHash24().hashString(id, StandardCharsets.US_ASCII).asInt();
    }

    public Address getExternalAddress() {
        return externalAddress;
    }

    public Address getInternalAddress() {
        return internalAddress;
    }

    public String getId() {
        return id;
    }

    public Versionstamp getProcessId() {
        return processId;
    }

    public MemberStatus getStatus() {
        return status;
    }

    public void setStatus(MemberStatus status) {
        this.status = status;
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            // Hacky?
            hashCode = sipHash24().hashString(id, StandardCharsets.US_ASCII).asInt();
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Member member)) {
            return false;
        }
        return member.getId().equals(id);
    }

    @Override
    public String toString() {
        return String.format(
                "Member {id=%s externalAddress=%s internalAddress=%s processId=%s status=%s}",
                id,
                externalAddress,
                internalAddress,
                VersionstampUtils.base32HexEncode(processId),
                status
        );
    }
}