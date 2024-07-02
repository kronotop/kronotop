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
import com.kronotop.JSONUtils;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class VolumeMetadata {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Set<Long> segments = new HashSet<>();
    private final List<Host> hosts = new ArrayList<>();
    @JsonProperty("segment_id")
    private long segmentId = 0;

    public static VolumeMetadata fromJSON(byte[] data) {
        return JSONUtils.readValue(data, VolumeMetadata.class);
    }

    @JsonIgnore
    public long getAndIncrementSegmentId() {
        lock.writeLock().lock();
        try {
            return segmentId;
        } finally {
            segmentId++;
            lock.writeLock().unlock();
        }
    }

    public void addSegment(long segmentId) {
        lock.writeLock().lock();
        try {
            segments.add(segmentId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeSegment(long segmentId) {
        lock.writeLock().lock();
        try {
            segments.remove(segmentId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Set<Long> getSegments() {
        lock.readLock().lock();
        try {
            return Collections.unmodifiableSet(segments);
        } finally {
            lock.readLock().unlock();
        }
    }

    @JsonIgnore
    public void setStandby(Host host) {
        lock.writeLock().lock();
        try {
            if (host.role() == Role.OWNER) {
                throw new IllegalArgumentException("Cannot set an owner host as a standby");
            }
            for (Host existing : hosts) {
                if (existing.equals(host)) {
                    return;
                }
            }
            hosts.add(host);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void unsetHost(Host host) {
        lock.writeLock().lock();
        try {
            for (int i = 0; i < hosts.size(); i++) {
                Host existing = hosts.get(i);
                if (existing.equals(host)) {
                    hosts.remove(i);
                    return;
                }
            }
            throw new IllegalStateException("Host not found");
        } finally {
            lock.writeLock().unlock();
        }
    }

    @JsonIgnore
    public void unsetStandby(Host host) {
        lock.writeLock().lock();
        try {
            if (host.role() == Role.OWNER) {
                throw new IllegalArgumentException("Host is an owner");
            }
            unsetHost(host);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @JsonIgnore
    public List<Host> getStandbyHosts() {
        lock.readLock().lock();
        try {
            return hosts.stream().filter(host -> host.role() == Role.STANDBY).toList();
        } finally {
            lock.readLock().unlock();
        }
    }

    @JsonIgnore
    public Host getOwner() {
        lock.readLock().lock();
        try {
            for (Host host : hosts) {
                if (host.role() == Role.OWNER) {
                    return host;
                }
            }
            throw new IllegalStateException("No owner host found");
        } finally {
            lock.readLock().unlock();
        }
    }

    @JsonIgnore
    public void setOwner(Host host) {
        lock.writeLock().lock();
        try {
            if (host.role() == Role.STANDBY) {
                throw new IllegalArgumentException("Cannot set a standby host as a owner");
            }
            for (Host existing : hosts) {
                if (existing.equals(host)) {
                    return;
                }
            }
            hosts.add(host);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @JsonIgnore
    public void unsetOwner(Host host) {
        lock.writeLock().lock();
        try {
            if (host.role() == Role.STANDBY) {
                throw new IllegalArgumentException("Host is a standby");
            }
            unsetHost(host);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<Host> getHosts() {
        lock.readLock().lock();
        try {
            return Collections.unmodifiableList(hosts);
        } finally {
            lock.readLock().unlock();
        }
    }

    public byte[] toByte() {
        lock.readLock().lock();
        try {
            return JSONUtils.writeValueAsBytes(this);
        } finally {
            lock.readLock().unlock();
        }
    }
}