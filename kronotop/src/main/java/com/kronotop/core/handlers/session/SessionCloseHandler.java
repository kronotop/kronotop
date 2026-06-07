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

import com.kronotop.Context;
import com.kronotop.core.handlers.session.protocol.SessionCloseMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.watcher.Watcher;

/**
 * Handler for the SESSION.CLOSE command that wipes out session state while keeping the connection open.
 * <p>
 * This command performs a full session reset:
 * <ul>
 *   <li>Clears all cursor maps (read, delete, update query contexts)</li>
 *   <li>Rolls back and closes any active FDB transaction</li>
 *   <li>Cleans up Redis MULTI transaction state</li>
 *   <li>Unwatches all watched keys</li>
 *   <li>Resets cursor ID counter to 1</li>
 *   <li>Resets all configuration attributes to defaults</li>
 * </ul>
 */
@Command(SessionCloseMessage.COMMAND)
@MaximumParameterCount(SessionCloseMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(SessionCloseMessage.MINIMUM_PARAMETER_COUNT)
public class SessionCloseHandler implements Handler {
    private final Watcher watcher;

    public SessionCloseHandler(Context context) {
        this.watcher = context.getService(Watcher.NAME);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SESSIONCLOSE).set(new SessionCloseMessage());
    }

    @Override
    public boolean isRedisCompatible() {
        return false;
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

        // 3. Reset Redis MULTI transaction state
        session.resetRedisTransactionState();

        // 4. Unwatch all watched keys
        watcher.unwatchWatchedKeys(session);

        // 5. Reset cursor ID counter to 1
        session.resetCursorId();

        // 6. Reset all configuration attributes to defaults
        session.resetSessionAttributes();

        response.writeOK();
    }
}
