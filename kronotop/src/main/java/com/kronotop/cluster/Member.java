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

import com.apple.foundationdb.tuple.Versionstamp;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.kronotop.VersionstampUtils;
import com.kronotop.network.Address;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;

import static com.google.common.hash.Hashing.sipHash24;

public class Member {
    private String id;
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
        if (id.isEmpty() || id.isBlank()) {
            throw new IllegalArgumentException("id cannot be blank or empty");
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

    @Override
    public int hashCode() {
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
        return member.getId().equals(id) && member.getProcessId().equals(processId);
    }

    @Override
    public String toString() {
        return String.format(
                "Member {id=%s externalAddress=%s internalAddress=%s processId=%s}",
                id,
                externalAddress,
                internalAddress,
                VersionstampUtils.base64Encode(processId)
        );
    }
}