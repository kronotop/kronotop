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

package com.kronotop.core.cluster.coordinator;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.kronotop.core.cluster.Member;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The RoutingTable class represents a routing table used in a distributed system. It maintains a mapping of shard identifiers to routes,
 * and keeps track of the coordinator member and version number.
 */
public class RoutingTable {
    @JsonSerialize(using = RoutesSerializer.class)
    @JsonDeserialize(using = RoutesDeserializer.class)
    private final HashMap<Integer, Route> routes = new HashMap<>();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private Long version = 0L;
    @JsonDeserialize(using = CoordinatorDeserializer.class)
    private Member coordinator;

    public RoutingTable() {
    }

    public Member getCoordinator() {
        readWriteLock.readLock().lock();
        try {
            return coordinator;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public void updateCoordinator(Member member) {
        readWriteLock.writeLock().lock();
        try {
            version++;
            coordinator = member;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public void setRoute(int shardId, Route route) {
        readWriteLock.writeLock().lock();
        try {
            version++;
            routes.put(shardId, route);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public Route getRoute(int shardId) {
        readWriteLock.readLock().lock();
        try {
            return routes.get(shardId);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public Long getVersion() {
        readWriteLock.readLock().lock();
        try {
            return version;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof RoutingTable)) {
            return false;
        }

        final RoutingTable routingTable = (RoutingTable) obj;
        return routingTable.getVersion().equals(routingTable.getVersion())
                && getCoordinator().equals(routingTable.getCoordinator());
    }

    @Override
    public String toString() {
        return String.format("RoutingTable {coordinator=%s version=%d}", getCoordinator(), getVersion());
    }

    private static class CoordinatorDeserializer extends JsonDeserializer<Member> {

        @Override
        public Member deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            return p.readValueAs(Member.class);
        }
    }

    private static class RoutesSerializer extends JsonSerializer<HashMap<Integer, Route>> {

        @Override
        public void serialize(HashMap<Integer, Route> value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeObject(value);
        }
    }

    private static class RoutesDeserializer extends JsonDeserializer<HashMap<Integer, Route>> {

        @Override
        public HashMap<Integer, Route> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            TypeReference<HashMap<Integer, Route>> typeRef = new TypeReference<>() {
            };
            return p.readValueAs(typeRef);
        }
    }
}
