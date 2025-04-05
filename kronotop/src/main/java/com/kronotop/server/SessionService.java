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

import com.kronotop.CommandHandlerService;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.server.handlers.SessionAttributeHandler;

// SessionService is a placeholder service to register SessionAttributeHandler
public class SessionService extends CommandHandlerService implements KronotopService {
    public static final String NAME = "Session";

    public SessionService(Context context) {
        super(context, NAME);

        handlerMethod(ServerKind.EXTERNAL, new SessionAttributeHandler());
        handlerMethod(ServerKind.INTERNAL, new SessionAttributeHandler());
    }
}
