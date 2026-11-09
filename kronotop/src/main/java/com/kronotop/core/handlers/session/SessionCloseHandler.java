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

package com.kronotop.core.handlers.session;

import com.kronotop.core.handlers.session.protocol.SessionCloseMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumArgumentCount;
import com.kronotop.server.annotation.MinimumArgumentCount;

/**
 * Handler for the SESSION.CLOSE command that wipes out session state while keeping the connection open.
 * <p>
 * This command performs a full session reset:
 * <ul>
 *   <li>Clears all cursor maps (read, delete, update query contexts)</li>
 *   <li>Rolls back and closes any active FDB transaction</li>
 *   <li>Resets cursor ID counter to 1</li>
 *   <li>Restores the session defaults: configuration attributes, snapshot read, current namespace
 *       and open namespace cache</li>
 * </ul>
 * <p>
 * State that belongs to the connection rather than the session is kept: the negotiated RESP version,
 * the authentication flag and the client attributes.
 */
@Command(SessionCloseMessage.COMMAND)
@MaximumArgumentCount(SessionCloseMessage.MAXIMUM_ARGUMENT_COUNT)
@MinimumArgumentCount(SessionCloseMessage.MINIMUM_ARGUMENT_COUNT)
public class SessionCloseHandler implements Handler {

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SESSIONCLOSE).set(new SessionCloseMessage());
    }

    @Override
    public boolean requiresClusterInitialization() {
        return false;
    }

    @Override
    public void execute(Request request, Response response) {
        Session session = request.getSession();

        // 1. Clear all cursors
        session.clearCursors();

        // 2. Rollback and close any active FDB transaction
        session.closeAndCleanupTransaction();

        // 3. Reset cursor ID counter to 1
        session.resetCursorId();

        // 4. Restore all session defaults
        session.restoreDefaults();

        response.writeOK();
    }
}
