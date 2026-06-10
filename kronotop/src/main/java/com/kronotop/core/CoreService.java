/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.core;

import com.kronotop.CommandHandlerService;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.core.handlers.InfoHandler;
import com.kronotop.core.handlers.client.ClientHandler;
import com.kronotop.core.handlers.connection.AuthHandler;
import com.kronotop.core.handlers.connection.EchoHandler;
import com.kronotop.core.handlers.connection.HelloHandler;
import com.kronotop.core.handlers.connection.PingHandler;
import com.kronotop.core.handlers.server.CommandHandler;
import com.kronotop.core.handlers.session.SessionAttributeHandler;
import com.kronotop.core.handlers.session.SessionCloseHandler;
import com.kronotop.core.handlers.transaction.*;
import com.kronotop.server.ServerKind;

public class CoreService extends CommandHandlerService implements KronotopService {
    public static final String NAME = "Core";

    public CoreService(Context context) {
        super(context, NAME);

        handlerMethod(ServerKind.EXTERNAL, new PingHandler());
        handlerMethod(ServerKind.EXTERNAL, new EchoHandler());
        handlerMethod(ServerKind.EXTERNAL, new AuthHandler(context));
        handlerMethod(ServerKind.EXTERNAL, new HelloHandler(context));
        handlerMethod(ServerKind.EXTERNAL, new ClientHandler());
        handlerMethod(ServerKind.EXTERNAL, new InfoHandler());
        handlerMethod(ServerKind.EXTERNAL, new CommandHandler(context));
        handlerMethod(ServerKind.EXTERNAL, new SessionAttributeHandler());
        handlerMethod(ServerKind.EXTERNAL, new SessionCloseHandler(context));

        // Transaction management
        handlerMethod(ServerKind.EXTERNAL, new BeginHandler(context));
        handlerMethod(ServerKind.EXTERNAL, new RollbackHandler(context));
        handlerMethod(ServerKind.EXTERNAL, new CommitHandler(context));
        handlerMethod(ServerKind.EXTERNAL, new SnapshotReadHandler(context));
        handlerMethod(ServerKind.EXTERNAL, new GetReadVersionHandler(context));
        handlerMethod(ServerKind.EXTERNAL, new GetApproximateSizeHandler(context));
        handlerMethod(ServerKind.EXTERNAL, new TickHandler(context));

        handlerMethod(ServerKind.INTERNAL, new PingHandler());
        handlerMethod(ServerKind.INTERNAL, new EchoHandler());
        handlerMethod(ServerKind.INTERNAL, new HelloHandler(context));
        handlerMethod(ServerKind.INTERNAL, new ClientHandler());
        handlerMethod(ServerKind.INTERNAL, new InfoHandler());
        handlerMethod(ServerKind.INTERNAL, new CommandHandler(context));
        handlerMethod(ServerKind.INTERNAL, new SessionAttributeHandler());
        handlerMethod(ServerKind.INTERNAL, new SessionCloseHandler(context));
    }

    @Override
    public String getName() {
        return NAME;
    }
}
