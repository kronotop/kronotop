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

package com.kronotop.foundationdb.zmap;

import com.kronotop.core.Context;
import com.kronotop.core.KronotopService;
import com.kronotop.server.CommandAlreadyRegisteredException;
import com.kronotop.server.Handler;
import com.kronotop.server.Handlers;
import com.kronotop.server.annotation.Command;

public class ZMapService implements KronotopService {
    public static final byte SubspaceMagic = 0x01;
    public static final String NAME = "ZMap";
    private final Context context;
    private final Handlers commands;

    public ZMapService(Context context, Handlers commands) {
        this.context = context;
        this.commands = commands;

        registerHandler(new ZPutHandler(this));
        registerHandler(new ZGetHandler(this));
        registerHandler(new ZDelHandler(this));
        registerHandler(new ZDelRangeHandler(this));
        registerHandler(new ZDelPrefixHandler(this));
        registerHandler(new ZGetRangeHandler(this));
        registerHandler(new ZGetKeyHandler(this));
        registerHandler(new ZMutateHandler(this));
        registerHandler(new ZGetRangeSizeHandler(this));
    }

    private void registerHandler(Handler... handlers) throws CommandAlreadyRegisteredException {
        for (Handler handler : handlers) {
            Command annotation = handler.getClass().getAnnotation(Command.class);
            commands.register(annotation.value().toUpperCase(), handler);
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Context getContext() {
        return this.context;
    }

    @Override
    public void shutdown() {
    }
}
