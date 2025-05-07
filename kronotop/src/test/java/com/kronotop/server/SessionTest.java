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


package com.kronotop.server;

import com.apple.foundationdb.Transaction;
import com.kronotop.BaseHandlerTest;
import com.kronotop.KronotopException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SessionTest extends BaseHandlerTest {

    @Test
    void test_session_registered() {
        Session session = Session.extractSessionFromChannel(channel);
        assertNotNull(session);
    }

    @Test
    void test_setTransaction() {
        Session session = Session.extractSessionFromChannel(channel);
        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            assertDoesNotThrow(() -> session.setTransaction(tr));

            assertEquals(true, session.attr(SessionAttributes.BEGIN).get());
            assertNull(session.attr(SessionAttributes.AUTO_COMMIT).get());
            assertEquals(0, session.attr(SessionAttributes.POST_COMMIT_HOOKS).get().size());
            assertEquals(0, session.attr(SessionAttributes.ASYNC_RETURNING).get().size());
            assertEquals(0, session.attr(SessionAttributes.USER_VERSION_COUNTER).get().get());
            assertNotNull(session.attr(SessionAttributes.TRANSACTION).get());
        }
    }

    @Test
    void test_setTransaction_many_times() {
        Session session = Session.extractSessionFromChannel(channel);
        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            session.setTransaction(tr);
            assertThrows(KronotopException.class, () -> session.setTransaction(tr));
        }
    }

    @Test
    void test_unsetTransaction() {
        Session session = Session.extractSessionFromChannel(channel);
        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            session.setTransaction(tr);
            assertDoesNotThrow(session::unsetTransaction);

            assertEquals(false, session.attr(SessionAttributes.BEGIN).get());
            assertEquals(false, session.attr(SessionAttributes.AUTO_COMMIT).get());
            assertEquals(0, session.attr(SessionAttributes.POST_COMMIT_HOOKS).get().size());
            assertEquals(0, session.attr(SessionAttributes.ASYNC_RETURNING).get().size());
            assertEquals(0, session.attr(SessionAttributes.USER_VERSION_COUNTER).get().get());
            assertNull(session.attr(SessionAttributes.TRANSACTION).get());
        }
    }

    @Test
    void test_channelUnregistered() {
        Session session = Session.extractSessionFromChannel(channel);
        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            session.setTransaction(tr);
            session.channelUnregistered();
            assertNull(session.attr(SessionAttributes.TRANSACTION).get());
        }
    }

    @Test
    void test_closeTransactionIfAny() {
        Session session = Session.extractSessionFromChannel(channel);
        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            session.setTransaction(tr);
            session.closeTransactionIfAny();
            assertNull(session.attr(SessionAttributes.TRANSACTION).get());
        }
    }

    @Test
    void test_cleanupIfAutoCommitEnabled() {
        Session session = Session.extractSessionFromChannel(channel);
        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            session.setTransaction(tr);
            session.attr(SessionAttributes.AUTO_COMMIT).set(true);
            session.cleanupIfAutoCommitEnabled();

            assertEquals(false, session.attr(SessionAttributes.BEGIN).get());
            assertEquals(false, session.attr(SessionAttributes.AUTO_COMMIT).get());
            assertEquals(0, session.attr(SessionAttributes.POST_COMMIT_HOOKS).get().size());
            assertEquals(0, session.attr(SessionAttributes.ASYNC_RETURNING).get().size());
            assertEquals(0, session.attr(SessionAttributes.USER_VERSION_COUNTER).get().get());
            assertNull(session.attr(SessionAttributes.TRANSACTION).get());
        }
    }

    @Test
    void test_getClientId() {
        Session first = Session.extractSessionFromChannel(channel);
        assertTrue(first.getClientId() >= 0);
    }
}