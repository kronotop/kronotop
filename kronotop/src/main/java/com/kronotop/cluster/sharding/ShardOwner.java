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

package com.kronotop.cluster.sharding;

import com.apple.foundationdb.tuple.Versionstamp;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.kronotop.internal.VersionstampDeserializer;
import com.kronotop.internal.VersionstampSerializer;
import com.kronotop.network.Address;
import com.kronotop.network.AddressDeserializer;

public class ShardOwner {
    @JsonSerialize(using = VersionstampSerializer.class)
    @JsonDeserialize(using = VersionstampDeserializer.class)
    private Versionstamp processId;
    @JsonDeserialize(using = AddressDeserializer.class)
    private Address address;

    ShardOwner() {
    }

    public ShardOwner(Address address, Versionstamp processId) {
        this.address = address;
        this.processId = processId;
    }

    public Address getAddress() {
        return address;
    }

    public void setAddress(Address address) {
        this.address = address;
    }

    public Versionstamp getProcessId() {
        return processId;
    }

    public void setProcessId(Versionstamp processId) {
        this.processId = processId;
    }

    @Override
    public String toString() {
        return String.format(
                "ShardOwner {address=%s processId=%s}",
                address,
                processId
        );
    }
}
