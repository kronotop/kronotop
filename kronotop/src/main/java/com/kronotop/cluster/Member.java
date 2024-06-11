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

package com.kronotop.cluster;

import com.apple.foundationdb.tuple.Versionstamp;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.kronotop.VersionstampUtils;
import com.kronotop.network.Address;
import io.netty.util.CharsetUtil;

import java.nio.charset.StandardCharsets;

import static com.google.common.hash.Hashing.murmur3_32_fixed;

public class Member {
    private String id;
    private Address address;
    @JsonSerialize(using = ProcessIdSerializer.class)
    @JsonDeserialize(using = ProcessIdDeserializer.class)
    private Versionstamp processId;

    Member() {
    }

    public Member(Address address, Versionstamp processId) {
        this.address = address;
        this.processId = processId;
        HashCode hashCode = Hashing.sha1().newHasher().
                putString(address.getHost(), CharsetUtil.US_ASCII).
                putInt(address.getPort()).
                hash();
        this.id = hashCode.toString();
    }

    public Address getAddress() {
        return address;
    }

    public String getId() {
        return id;
    }

    public Versionstamp getProcessId() {
        return processId;
    }

    @Override
    public int hashCode() {
        return murmur3_32_fixed().hashString(id, StandardCharsets.US_ASCII).asInt();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Member)) {
            return false;
        }
        final Member member = (Member) obj;
        return member.getId().equals(id) && member.getProcessId().equals(processId);
    }

    @Override
    public String toString() {
        return String.format(
                "Member {id=%s address=%s processId=%s}",
                id,
                address,
                VersionstampUtils.base64Encode(processId)
        );
    }
}