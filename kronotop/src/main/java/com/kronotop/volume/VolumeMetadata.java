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

package com.kronotop.volume;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

public class VolumeMetadata {
    private final Set<Long> segments = new HashSet<>();
    @JsonProperty("segment_id")
    private long segmentId = 0;
    private final List<Host> hosts = new ArrayList<>();

    @JsonIgnore
    public long getAndIncrementSegmentId() {
        try {
            return segmentId;
        } finally {
            segmentId++;
        }
    }

    public void addSegment(long segmentId) {
        segments.add(segmentId);
    }

    public Set<Long> getSegments() {
        return Collections.unmodifiableSet(segments);
    }

    @JsonIgnore
    public void setStandby(Host host) {
        if (host.role() == Role.OWNER) {
            throw new IllegalArgumentException("Cannot set an owner host as a standby");
        }
        for (Host existing : hosts) {
            if (existing.equals(host)) {
                return;
            }
        }
        hosts.add(host);
    }

    private void unsetHost(Host host) {
        for (int i = 0; i < hosts.size(); i++) {
            Host existing = hosts.get(i);
            if (existing.equals(host)) {
                hosts.remove(i);
                return;
            }
        }
        throw new IllegalStateException("Host not found");
    }

    @JsonIgnore
    public void unsetStandby(Host host) {
        if (host.role() == Role.OWNER) {
            throw new IllegalArgumentException("Host is an owner");
        }
        unsetHost(host);
    }

    @JsonIgnore
    public List<Host> getStandbyHosts() {
        return hosts.stream().filter(host -> host.role() == Role.STANDBY).toList();
    }

    @JsonIgnore
    public void setOwner(Host host) {
        if (host.role() == Role.STANDBY) {
            throw new IllegalArgumentException("Cannot set a standby host as a owner");
        }
        for (Host existing : hosts) {
            if (existing.equals(host)) {
                return;
            }
        }
        hosts.add(host);
    }

    @JsonIgnore
    public Host getOwner() {
        for (Host host : hosts) {
            if (host.role() == Role.OWNER) {
                return host;
            }
        }
        throw new IllegalStateException("No owner host found");
    }

    @JsonIgnore
    public void unsetOwner(Host host) {
        if (host.role() == Role.STANDBY) {
            throw new IllegalArgumentException("Host is a standby");
        }
        unsetHost(host);
    }

    public List<Host> getHosts() {
        return Collections.unmodifiableList(hosts);
    }
}