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

import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.namespace.handlers.Namespace;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SessionStoreTest extends BaseStandaloneInstanceTest {

    @Test
    void shouldPutAndRemoveSession() {
        SessionStore store = new SessionStore();
        Session session = getSession();
        long clientId = session.getClientId();

        store.put(clientId, session);
        store.remove(clientId);
    }

    @Test
    void shouldInvalidateExactNamespace() {
        SessionStore store = new SessionStore();
        Session session = getSession();
        long clientId = session.getClientId();

        Map<String, Namespace> openNamespaces = session.attr(SessionAttributes.OPEN_NAMESPACES).get();
        openNamespaces.put("a.b", new Namespace());

        store.put(clientId, session);
        store.invalidateOpenNamespaces("a.b");

        assertFalse(openNamespaces.containsKey("a.b"));
    }

    @Test
    void shouldInvalidateChildNamespaces() {
        SessionStore store = new SessionStore();
        Session session = getSession();
        long clientId = session.getClientId();

        Map<String, Namespace> openNamespaces = session.attr(SessionAttributes.OPEN_NAMESPACES).get();
        openNamespaces.put("a.b", new Namespace());
        openNamespaces.put("a.b.c", new Namespace());
        openNamespaces.put("a.b.c.d", new Namespace());
        openNamespaces.put("a.b2", new Namespace());

        store.put(clientId, session);
        store.invalidateOpenNamespaces("a.b");

        assertFalse(openNamespaces.containsKey("a.b"));
        assertFalse(openNamespaces.containsKey("a.b.c"));
        assertFalse(openNamespaces.containsKey("a.b.c.d"));
        assertTrue(openNamespaces.containsKey("a.b2"));
    }

    @Test
    void shouldHandleEmptyStore() {
        SessionStore store = new SessionStore();
        assertDoesNotThrow(() -> store.invalidateOpenNamespaces("a.b"));
    }
}
