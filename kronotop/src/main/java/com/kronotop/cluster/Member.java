/*
 * Copyright (c) 2023-2026 Burak Sezer
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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.kronotop.internal.VersionstampDeserializer;
import com.kronotop.internal.VersionstampSerializer;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.network.Address;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A single node of a Kronotop cluster. Bind addresses say where the node listens,
 * advertise lists say how clients and other members reach it. Members are equal
 * when their ids are equal.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Member {
    private String id;
    private MemberStatus status = MemberStatus.UNKNOWN;
    private Address externalAddress;
    private Address internalAddress;
    private List<Address> externalAdvertise;
    private List<Address> internalAdvertise;
    @JsonSerialize(using = VersionstampSerializer.class)
    @JsonDeserialize(using = VersionstampDeserializer.class)
    private Versionstamp processId;

    Member() {
    }

    public Member(
            @Nonnull String id,
            @Nonnull Address externalAddress,
            @Nonnull Address internalAddress,
            @Nonnull List<Address> externalAdvertise,
            @Nonnull List<Address> internalAdvertise,
            @Nonnull Versionstamp processId
    ) {
        if (id.isBlank()) {
            throw new IllegalArgumentException("id cannot be blank");
        }
        if (externalAdvertise.isEmpty()) {
            throw new IllegalArgumentException("externalAdvertise cannot be empty");
        }
        if (internalAdvertise.isEmpty()) {
            throw new IllegalArgumentException("internalAdvertise cannot be empty");
        }

        this.id = id;
        this.externalAddress = externalAddress;
        this.internalAddress = internalAddress;
        this.externalAdvertise = externalAdvertise;
        this.internalAdvertise = internalAdvertise;
        this.processId = processId;
    }

    /**
     * Bind address of the client-facing server. Not reachable from outside.
     * Use {@link #primaryExternalAdvertise()} to give a client an address.
     */
    public Address getExternalAddress() {
        return externalAddress;
    }

    /**
     * Bind address of the member-facing server. Not reachable from outside.
     * Use {@link #primaryInternalAdvertise()} to reach another member.
     */
    public Address getInternalAddress() {
        return internalAddress;
    }

    /**
     * Addresses clients can use to reach this member, most preferred first.
     */
    public List<Address> getExternalAdvertise() {
        return externalAdvertise;
    }

    /**
     * Addresses other members can use to reach this member, most preferred first.
     */
    public List<Address> getInternalAdvertise() {
        return internalAdvertise;
    }

    /**
     * Returns the preferred address for client connections.
     */
    public Address primaryExternalAdvertise() {
        return externalAdvertise.getFirst();
    }

    /**
     * Returns the preferred address for connections coming from other cluster members.
     */
    public Address primaryInternalAdvertise() {
        return internalAdvertise.getFirst();
    }

    /**
     * Returns the cluster-wide unique id of this member.
     */
    public String getId() {
        return id;
    }

    /**
     * Returns the id of the running process. It changes on every restart.
     */
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
        return id.hashCode();
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
                VersionstampUtil.base32HexEncode(processId),
                status
        );
    }
}