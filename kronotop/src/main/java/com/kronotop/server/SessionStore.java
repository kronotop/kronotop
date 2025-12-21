/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.server;

import com.kronotop.namespace.handlers.Namespace;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.StampedLock;

/**
 * Thread-safe store for managing client sessions.
 */
public class SessionStore {
    private final StampedLock lock = new StampedLock();
    private final Map<Long, Session> sessions = new HashMap<>();

    /**
     * Registers a session for the given client ID if not already present.
     *
     * @param clientId the client identifier
     * @param session  the session to register
     */
    public void put(long clientId, Session session) {
        long stamp = lock.writeLock();
        try {
            sessions.putIfAbsent(clientId, session);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Removes the session for the given client ID.
     *
     * @param clientId the client identifier
     */
    public void remove(long clientId) {
        long stamp = lock.writeLock();
        try {
            sessions.remove(clientId);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Invalidates open namespaces across all sessions. Removes the exact namespace
     * and all child namespaces (e.g., invalidating "a.b" also removes "a.b.c.d").
     *
     * @param namespace the namespace prefix to invalidate
     */
    public void invalidateOpenNamespaces(String namespace) {
        long stamp = lock.writeLock();
        try {
            String childPrefix = namespace + ".";
            for (Session session : sessions.values()) {
                Map<String, Namespace> openNamespaces = session.attr(SessionAttributes.OPEN_NAMESPACES).get();
                openNamespaces.keySet().removeIf(ns -> ns.equals(namespace) || ns.startsWith(childPrefix));
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }
}
