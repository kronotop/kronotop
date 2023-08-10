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

package com.kronotop.server.resp;

import com.kronotop.common.Preconditions;

import java.util.HashMap;
import java.util.Set;

public class Handlers {
    private final HashMap<String, Handler> handlers;

    public Handlers() {
        handlers = new HashMap<>();
    }

    public void register(String command, Handler handler) throws CommandAlreadyRegisteredException {
        Preconditions.checkNotNull(handler, "handler cannot be null");
        if (handlers.containsKey(command)) {
            throw new CommandAlreadyRegisteredException(String.format("command already registered '%s'", command));
        }
        handlers.put(command, handler);
    }

    public Handler get(String command) throws CommandNotFoundException {
        Handler handler = handlers.get(command);
        if (handler == null) {
            throw new CommandNotFoundException(String.format("unknown command '%s'", command));
        }
        return handler;
    }

    public Set<String> getCommands() {
        return handlers.keySet();
    }
}
